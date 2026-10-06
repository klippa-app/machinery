package redis_test

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/machinery/v1/brokers/errs"
	"github.com/RichardKnop/machinery/v1/brokers/redis"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
)

func newBroker(t *testing.T, redisURL, queue, delayedKey string) *redis.Broker {
	b := redis.New(testConfig(queue, delayedKey), []string{redisURL}, 0, "")
	broker, ok := b.(*redis.Broker)
	require.True(t, ok)
	return broker
}

func TestNew(t *testing.T) {
	redisURL := getRedisURL(t)
	broker := newBroker(t, redisURL, "default_queue", "")
	assert.NotNil(t, broker)
	assert.Equal(t, "default_queue", broker.GetConfig().DefaultQueue)
	broker.StopConsuming()
}

func TestNew_AddressParsingVariants(t *testing.T) {
	redisURL := getRedisURL(t)

	cnfWithMaster := testConfig("q", "")
	cnfWithMaster.Redis.MasterName = "mymaster"

	variants := []struct {
		name  string
		cnf   *config.Config
		addrs []string
	}{
		{"plain", testConfig("q", ""), []string{redisURL}},
		{"password-only", testConfig("q", ""), []string{"somepass@" + redisURL}},
		{"user-and-password", testConfig("q", ""), []string{"someuser:somepass@" + redisURL}},
		{"master-name", cnfWithMaster, []string{redisURL}},
	}

	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			b := redis.New(v.cnf, append([]string(nil), v.addrs...), 0, "")
			broker, ok := b.(*redis.Broker)
			require.True(t, ok)
			assert.NotNil(t, broker)
			broker.StopConsuming()
		})
	}
}

func TestNew_ClusterMode(t *testing.T) {
	redisURL := getRedisURL(t)
	cnf := testConfig("q", "")
	cnf.Redis.ClusterMode = true

	b := redis.New(cnf, []string{redisURL}, 0, "")
	broker, ok := b.(*redis.Broker)
	require.True(t, ok)
	assert.NotNil(t, broker)
	broker.StopConsuming()
}

func TestPublishAndGetPendingTasks(t *testing.T) {
	redisURL := getRedisURL(t)
	queue := uniqueName(t, "queue")
	broker := newBroker(t, redisURL, queue, "")
	defer broker.StopConsuming()

	client := rawRedisClient(redisURL)
	defer client.Close()
	defer client.Del(ctxBg(), queue)

	sig := &tasks.Signature{UUID: "task1", Name: "add"}
	err := broker.Publish(context.Background(), sig)
	assert.NoError(t, err)
	assert.Equal(t, queue, sig.RoutingKey)

	pending, err := broker.GetPendingTasks(queue)
	assert.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, "task1", pending[0].UUID)

	count, err := broker.GetPendingTaskCount(queue)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, count)

	pending, err = broker.GetPendingTasks("")
	assert.NoError(t, err)
	assert.Len(t, pending, 1)

	count, err = broker.GetPendingTaskCount("")
	assert.NoError(t, err)
	assert.EqualValues(t, 1, count)
}

func TestPublishMarshalError(t *testing.T) {
	redisURL := getRedisURL(t)
	queue := uniqueName(t, "queue")
	broker := newBroker(t, redisURL, queue, "")
	defer broker.StopConsuming()

	sig := &tasks.Signature{
		UUID: "task1",
		Name: "add",
		Headers: tasks.Headers{
			"bad": make(chan int),
		},
	}
	err := broker.Publish(context.Background(), sig)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "JSON marshal error")
}

func TestPublishDelayedAndCancel(t *testing.T) {
	redisURL := getRedisURL(t)
	queue := uniqueName(t, "queue")
	delayedKey := uniqueName(t, "delayed")
	broker := newBroker(t, redisURL, queue, delayedKey)
	defer broker.StopConsuming()

	client := rawRedisClient(redisURL)
	defer client.Close()
	defer client.Del(ctxBg(), delayedKey)

	eta := time.Now().Add(time.Hour)
	sig := &tasks.Signature{UUID: "delayed1", Name: "add", ETA: &eta}
	err := broker.Publish(context.Background(), sig)
	assert.NoError(t, err)

	delayed, err := broker.GetDelayedTasks()
	assert.NoError(t, err)
	require.Len(t, delayed, 1)
	assert.Equal(t, "delayed1", delayed[0].UUID)

	found, err := broker.GetDelayedTask("delayed1")
	assert.NoError(t, err)
	require.NotNil(t, found)
	assert.Equal(t, "delayed1", found.UUID)

	_, err = broker.GetDelayedTask("does-not-exist")
	assert.EqualError(t, err, "Not found")

	err = broker.CancelDelayedTask("delayed1")
	assert.NoError(t, err)

	delayed, err = broker.GetDelayedTasks()
	assert.NoError(t, err)
	assert.Len(t, delayed, 0)

	err = broker.CancelDelayedTask("delayed1")
	assert.EqualError(t, err, "Not found")
}

func TestGetPendingTasksRedisErrors(t *testing.T) {
	redisURL := getRedisURL(t)
	queue := uniqueName(t, "queue")
	broker := newBroker(t, redisURL, queue, "")
	defer broker.StopConsuming()

	client := rawRedisClient(redisURL)
	defer client.Close()
	defer client.Del(ctxBg(), queue)

	require.NoError(t, client.Set(ctxBg(), queue, "not-a-list", 0).Err())

	_, err := broker.GetPendingTasks(queue)
	assert.Error(t, err)

	_, err = broker.GetPendingTaskCount(queue)
	assert.Error(t, err)

	require.NoError(t, client.Del(ctxBg(), queue).Err())
	require.NoError(t, client.RPush(ctxBg(), queue, "not-json").Err())

	_, err = broker.GetPendingTasks(queue)
	assert.Error(t, err)
}

func TestGetDelayedTasksRedisErrors(t *testing.T) {
	redisURL := getRedisURL(t)
	queue := uniqueName(t, "queue")
	delayedKey := uniqueName(t, "delayed")
	broker := newBroker(t, redisURL, queue, delayedKey)
	defer broker.StopConsuming()

	client := rawRedisClient(redisURL)
	defer client.Close()
	defer client.Del(ctxBg(), delayedKey)

	require.NoError(t, client.Set(ctxBg(), delayedKey, "not-a-zset", 0).Err())

	_, err := broker.GetDelayedTasks()
	assert.Error(t, err)

	require.NoError(t, client.Del(ctxBg(), delayedKey).Err())
	require.NoError(t, client.ZAdd(ctxBg(), delayedKey, goredis.Z{Score: 1, Member: "not-json"}).Err())

	_, err = broker.GetDelayedTasks()
	assert.Error(t, err)
}

func TestStartConsumingPingFailureWithRetry(t *testing.T) {
	broker := newBroker(t, "127.0.0.1:1", "queue", "")
	defer broker.StopConsuming()
	processor := &testTaskProcessor{}

	retry, err := broker.StartConsuming("tag", 1, processor)
	assert.True(t, retry)
	assert.Error(t, err)
}

func TestStartConsumingPingFailureAfterStop(t *testing.T) {
	broker := newBroker(t, "127.0.0.1:1", "queue", "")
	processor := &testTaskProcessor{}

	broker.StopConsuming()

	retry, err := broker.StartConsuming("tag", 1, processor)
	assert.False(t, retry)
	assert.Equal(t, errs.ErrConsumerStopped, err)
}

func TestStartConsumingSuccessAndStop(t *testing.T) {
	redisURL := getRedisURL(t)
	queue := uniqueName(t, "queue")
	broker := newBroker(t, redisURL, queue, uniqueName(t, "delayed"))
	broker.SetRegisteredTaskNames([]string{"add"})

	client := rawRedisClient(redisURL)
	defer client.Close()
	defer client.Del(ctxBg(), queue)

	processed := make(chan *tasks.Signature, 10)
	processor := &testTaskProcessor{processed: processed}

	done := make(chan struct{})
	go func() {
		broker.StartConsuming("tag", 0, processor)
		close(done)
	}()

	sig := &tasks.Signature{UUID: "task1", Name: "add", RoutingKey: queue}
	require.NoError(t, broker.Publish(context.Background(), sig))

	select {
	case got := <-processed:
		assert.Equal(t, "task1", got.UUID)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for task to be processed")
	}

	broker.StopConsuming()
	<-done
}

func TestStartConsumingCustomQueue(t *testing.T) {
	redisURL := getRedisURL(t)
	customQueue := uniqueName(t, "customqueue")
	broker := newBroker(t, redisURL, uniqueName(t, "default"), uniqueName(t, "delayed"))
	broker.SetRegisteredTaskNames([]string{"add"})

	client := rawRedisClient(redisURL)
	defer client.Close()
	defer client.Del(ctxBg(), customQueue)

	processed := make(chan *tasks.Signature, 10)
	processor := &testTaskProcessor{customQueue: customQueue, processed: processed}

	done := make(chan struct{})
	go func() {
		broker.StartConsuming("tag", 2, processor)
		close(done)
	}()

	sig := &tasks.Signature{UUID: "task-custom", Name: "add", RoutingKey: customQueue}
	require.NoError(t, broker.Publish(context.Background(), sig))

	select {
	case got := <-processed:
		assert.Equal(t, "task-custom", got.UUID)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for task to be processed")
	}

	broker.StopConsuming()
	<-done
}

func TestStartConsumingProcessingError(t *testing.T) {
	redisURL := getRedisURL(t)
	queue := uniqueName(t, "queue")
	broker := newBroker(t, redisURL, queue, uniqueName(t, "delayed"))
	broker.SetRegisteredTaskNames([]string{"add"})

	client := rawRedisClient(redisURL)
	defer client.Close()
	defer client.Del(ctxBg(), queue)

	processError := assert.AnError
	processor := &testTaskProcessor{
		processFunc: func(sig *tasks.Signature) error {
			return processError
		},
	}

	done := make(chan struct{})
	var retry bool
	var startErr error
	go func() {
		retry, startErr = broker.StartConsuming("tag", 1, processor)
		close(done)
	}()

	sig := &tasks.Signature{UUID: "task-err", Name: "add", RoutingKey: queue}
	require.NoError(t, broker.Publish(context.Background(), sig))

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for StartConsuming to return")
	}
	assert.True(t, retry)
	assert.Equal(t, processError, startErr)

	broker.StopConsuming()
}

func TestStartConsumingNotRegisteredIgnored(t *testing.T) {
	redisURL := getRedisURL(t)
	queue := uniqueName(t, "queue")
	broker := newBroker(t, redisURL, queue, uniqueName(t, "delayed"))
	// Nothing registered.

	client := rawRedisClient(redisURL)
	defer client.Close()
	defer client.Del(ctxBg(), queue)

	processor := &testTaskProcessor{}

	done := make(chan struct{})
	go func() {
		broker.StartConsuming("tag", 1, processor)
		close(done)
	}()

	sig := &tasks.Signature{UUID: "task-unreg", Name: "unknown", RoutingKey: queue, IgnoreWhenTaskNotRegistered: true}
	require.NoError(t, broker.Publish(context.Background(), sig))

	time.Sleep(300 * time.Millisecond)

	count, err := broker.GetPendingTaskCount(queue)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, count)

	broker.StopConsuming()
	<-done
}

func TestStartConsumingNotRegisteredRequeued(t *testing.T) {
	redisURL := getRedisURL(t)
	queue := uniqueName(t, "queue")
	broker := newBroker(t, redisURL, queue, uniqueName(t, "delayed"))
	// Nothing registered, and IgnoreWhenTaskNotRegistered defaults to false.

	client := rawRedisClient(redisURL)
	defer client.Close()
	defer client.Del(ctxBg(), queue)

	processor := &testTaskProcessor{}

	done := make(chan struct{})
	go func() {
		broker.StartConsuming("tag", 1, processor)
		close(done)
	}()

	sig := &tasks.Signature{UUID: "task-requeue", Name: "unknown", RoutingKey: queue}
	require.NoError(t, broker.Publish(context.Background(), sig))

	time.Sleep(150 * time.Millisecond)
	broker.StopConsuming()
	<-done

	count, err := client.LLen(ctxBg(), queue).Result()
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, count, int64(1))
}

func TestStartConsumingPreConsumeHandlerFalse(t *testing.T) {
	redisURL := getRedisURL(t)
	queue := uniqueName(t, "queue")
	broker := newBroker(t, redisURL, queue, uniqueName(t, "delayed"))
	broker.SetRegisteredTaskNames([]string{"add"})

	client := rawRedisClient(redisURL)
	defer client.Close()
	defer client.Del(ctxBg(), queue)

	processor := &testTaskProcessor{preConsume: func() bool { return false }}

	done := make(chan struct{})
	go func() {
		broker.StartConsuming("tag", 1, processor)
		close(done)
	}()

	sig := &tasks.Signature{UUID: "task-held", Name: "add", RoutingKey: queue}
	require.NoError(t, broker.Publish(context.Background(), sig))

	time.Sleep(200 * time.Millisecond)

	count, err := broker.GetPendingTaskCount(queue)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, count)

	broker.StopConsuming()
	<-done
}

func TestStopConsumingWithoutStart(t *testing.T) {
	broker := newBroker(t, "127.0.0.1:1", "queue", "")
	assert.NotPanics(t, func() {
		broker.StopConsuming()
	})
}

func TestStartConsumingDelayedTask(t *testing.T) {
	redisURL := getRedisURL(t)
	queue := uniqueName(t, "queue")
	delayedKey := uniqueName(t, "delayed")
	broker := newBroker(t, redisURL, queue, delayedKey)
	broker.SetRegisteredTaskNames([]string{"add"})

	client := rawRedisClient(redisURL)
	defer client.Close()
	defer client.Del(ctxBg(), queue)
	defer client.Del(ctxBg(), delayedKey)

	processed := make(chan *tasks.Signature, 10)
	processor := &testTaskProcessor{processed: processed}

	done := make(chan struct{})
	go func() {
		broker.StartConsuming("tag", 1, processor)
		close(done)
	}()

	eta := time.Now().Add(100 * time.Millisecond)
	sig := &tasks.Signature{UUID: "task-delayed", Name: "add", RoutingKey: queue, ETA: &eta}
	require.NoError(t, broker.Publish(context.Background(), sig))

	select {
	case got := <-processed:
		assert.Equal(t, "task-delayed", got.UUID)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for delayed task to be processed")
	}

	broker.StopConsuming()
	<-done
}
