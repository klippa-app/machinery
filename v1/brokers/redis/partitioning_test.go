package redis_test

import (
	"context"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/machinery/v1/brokers/redis"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
)

func newBrokerWithConfig(t *testing.T, cnf *config.Config, redisURL string) *redis.Broker {
	b := redis.New(cnf, []string{redisURL}, 0, "")
	broker, ok := b.(*redis.Broker)
	require.True(t, ok)
	return broker
}

func partitionedConfig(defaultQueue, delayedKey string, pcnf *config.PartitioningConfig) *config.Config {
	cnf := testConfig(defaultQueue, delayedKey)
	cnf.Redis.Partitioning = pcnf
	return cnf
}

func testPartitionListKey(queue, partitionKey string) string {
	return queue + ":partition:" + partitionKey
}

func testPartitionsSetKey(queue string) string {
	return queue + ":partitions"
}

func testPartitionLeaseKey(queue, partitionKey string) string {
	return testPartitionListKey(queue, partitionKey) + ":leases"
}

func TestPublishPartitioned(t *testing.T) {
	redisURL := getRedisURL(t)
	queue := uniqueName(t, "queue")
	broker := newBrokerWithConfig(t, partitionedConfig(queue, "", &config.PartitioningConfig{}), redisURL)
	defer broker.StopConsuming()

	client := rawRedisClient(redisURL)
	defer client.Close()
	defer client.Del(ctxBg(), queue, testPartitionListKey(queue, "customer-a"), testPartitionListKey(queue, "customer-b"), testPartitionsSetKey(queue))

	sigA := &tasks.Signature{UUID: "task-a", Name: "add", PartitionKey: "customer-a"}
	sigB := &tasks.Signature{UUID: "task-b", Name: "add", PartitionKey: "customer-b"}
	sigPlain := &tasks.Signature{UUID: "task-plain", Name: "add"}

	require.NoError(t, broker.Publish(context.Background(), sigA))
	require.NoError(t, broker.Publish(context.Background(), sigB))
	require.NoError(t, broker.Publish(context.Background(), sigPlain))

	// Each partitioned task lands on its own list, not the plain queue.
	plainLen, err := client.LLen(ctxBg(), queue).Result()
	require.NoError(t, err)
	assert.EqualValues(t, 1, plainLen)

	aLen, err := client.LLen(ctxBg(), testPartitionListKey(queue, "customer-a")).Result()
	require.NoError(t, err)
	assert.EqualValues(t, 1, aLen)

	bLen, err := client.LLen(ctxBg(), testPartitionListKey(queue, "customer-b")).Result()
	require.NoError(t, err)
	assert.EqualValues(t, 1, bLen)

	// The partitions registry tracks every partition key seen.
	members, err := client.SMembers(ctxBg(), testPartitionsSetKey(queue)).Result()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"customer-a", "customer-b"}, members)

	// GetPendingTasks/GetPendingTaskCount aggregate across the plain queue
	// and every partition.
	pending, err := broker.GetPendingTasks(context.Background(), queue)
	require.NoError(t, err)
	assert.Len(t, pending, 3)

	count, err := broker.GetPendingTaskCount(context.Background(), queue)
	require.NoError(t, err)
	assert.EqualValues(t, 3, count)
}

func TestPublishPartitionKeyIgnoredWithoutPartitioningConfig(t *testing.T) {
	redisURL := getRedisURL(t)
	queue := uniqueName(t, "queue")
	// No Partitioning config set: PartitionKey must be ignored entirely, so
	// behavior is identical to pre-partitioning versions of this broker.
	broker := newBroker(t, redisURL, queue, "")
	defer broker.StopConsuming()

	client := rawRedisClient(redisURL)
	defer client.Close()
	defer client.Del(ctxBg(), queue, testPartitionListKey(queue, "customer-a"))

	sig := &tasks.Signature{UUID: "task-a", Name: "add", PartitionKey: "customer-a"}
	require.NoError(t, broker.Publish(context.Background(), sig))

	plainLen, err := client.LLen(ctxBg(), queue).Result()
	require.NoError(t, err)
	assert.EqualValues(t, 1, plainLen)

	partLen, err := client.LLen(ctxBg(), testPartitionListKey(queue, "customer-a")).Result()
	require.NoError(t, err)
	assert.EqualValues(t, 0, partLen)
}

// concurrencyProcessor records, per partition key, how many tasks were ever
// simultaneously in Process(), and the time at which each task started
// processing (relative to when the processor was created).
type concurrencyProcessor struct {
	customQueue string
	blockFor    time.Duration
	processed   chan *tasks.Signature

	start time.Time

	mu          sync.Mutex
	inFlight    map[string]int
	maxInFlight map[string]int
	startedAt   map[string]time.Duration
}

func newConcurrencyProcessor(blockFor time.Duration) *concurrencyProcessor {
	return &concurrencyProcessor{
		blockFor:    blockFor,
		processed:   make(chan *tasks.Signature, 100),
		start:       time.Now(),
		inFlight:    make(map[string]int),
		maxInFlight: make(map[string]int),
		startedAt:   make(map[string]time.Duration),
	}
}

func (p *concurrencyProcessor) Process(ctx context.Context, sig *tasks.Signature) error {
	p.mu.Lock()
	p.inFlight[sig.PartitionKey]++
	if p.inFlight[sig.PartitionKey] > p.maxInFlight[sig.PartitionKey] {
		p.maxInFlight[sig.PartitionKey] = p.inFlight[sig.PartitionKey]
	}
	p.startedAt[sig.UUID] = time.Since(p.start)
	p.mu.Unlock()

	time.Sleep(p.blockFor)

	p.mu.Lock()
	p.inFlight[sig.PartitionKey]--
	p.mu.Unlock()

	p.processed <- sig
	return nil
}

func (p *concurrencyProcessor) CustomQueue() string     { return p.customQueue }
func (p *concurrencyProcessor) PreConsumeHandler() bool { return true }
func (p *concurrencyProcessor) maxFor(key string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.maxInFlight[key]
}
func (p *concurrencyProcessor) startTime(uuid string) time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.startedAt[uuid]
}

func TestStartConsumingPartitionLocalConcurrency(t *testing.T) {
	redisURL := getRedisURL(t)
	queue := uniqueName(t, "queue")
	broker := newBrokerWithConfig(t, partitionedConfig(queue, uniqueName(t, "delayed"), &config.PartitioningConfig{LocalConcurrency: 1}), redisURL)
	broker.SetRegisteredTaskNames([]string{"add"})

	client := rawRedisClient(redisURL)
	defer client.Close()
	defer client.Del(ctxBg(), queue, testPartitionListKey(queue, "A"), testPartitionListKey(queue, "B"), testPartitionsSetKey(queue))

	blockFor := 200 * time.Millisecond
	processor := newConcurrencyProcessor(blockFor)

	// Publish before StartConsuming: a BLPOP already in flight only watches
	// the partition keys known at the moment it started blocking, so a
	// partition's very first task can otherwise sit unnoticed for up to one
	// poll period (Redis only wakes a blocked BLPOP for keys it's already
	// watching). Publishing first ensures "A" and "B" are both registered
	// before the first BLPOP call is made.
	require.NoError(t, broker.Publish(context.Background(), &tasks.Signature{UUID: "a1", Name: "add", PartitionKey: "A"}))
	require.NoError(t, broker.Publish(context.Background(), &tasks.Signature{UUID: "a2", Name: "add", PartitionKey: "A"}))
	require.NoError(t, broker.Publish(context.Background(), &tasks.Signature{UUID: "a3", Name: "add", PartitionKey: "A"}))
	require.NoError(t, broker.Publish(context.Background(), &tasks.Signature{UUID: "b1", Name: "add", PartitionKey: "B"}))

	done := make(chan struct{})
	go func() {
		broker.StartConsuming("tag", 4, processor)
		close(done)
	}()

	for i := 0; i < 4; i++ {
		select {
		case <-processor.processed:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for all tasks to be processed")
		}
	}

	broker.StopConsuming()
	<-done

	// LocalConcurrency: 1 must have kept partition A fully serialized, no
	// matter how large the worker pool is.
	assert.Equal(t, 1, processor.maxFor("A"))

	// Partition B must not have been blocked behind A's backlog: it should
	// have started well before A's three (serialized, 200ms each) tasks
	// could possibly have finished.
	assert.Less(t, processor.startTime("b1"), blockFor)
}

func TestStartConsumingPartitionGlobalConcurrency(t *testing.T) {
	redisURL := getRedisURL(t)
	queue := uniqueName(t, "queue")
	broker := newBrokerWithConfig(t, partitionedConfig(queue, uniqueName(t, "delayed"), &config.PartitioningConfig{GlobalConcurrency: 1}), redisURL)
	broker.SetRegisteredTaskNames([]string{"add"})

	client := rawRedisClient(redisURL)
	defer client.Close()
	defer client.Del(ctxBg(), queue, testPartitionListKey(queue, "A"), testPartitionLeaseKey(queue, "A"), testPartitionsSetKey(queue))

	blockFor := 200 * time.Millisecond
	processor := newConcurrencyProcessor(blockFor)

	done := make(chan struct{})
	go func() {
		broker.StartConsuming("tag", 4, processor)
		close(done)
	}()

	require.NoError(t, broker.Publish(context.Background(), &tasks.Signature{UUID: "g1", Name: "add", PartitionKey: "A"}))
	require.NoError(t, broker.Publish(context.Background(), &tasks.Signature{UUID: "g2", Name: "add", PartitionKey: "A"}))

	for i := 0; i < 2; i++ {
		select {
		case <-processor.processed:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for all tasks to be processed")
		}
	}

	broker.StopConsuming()
	<-done

	assert.Equal(t, 1, processor.maxFor("A"))

	// The lease must be released once a task finishes.
	leaseCount, err := client.ZCard(ctxBg(), testPartitionLeaseKey(queue, "A")).Result()
	require.NoError(t, err)
	assert.EqualValues(t, 0, leaseCount)
}

func TestNextPartitionedTaskLeaseExpirySelfHeals(t *testing.T) {
	redisURL := getRedisURL(t)
	queue := uniqueName(t, "queue")
	broker := newBrokerWithConfig(t, partitionedConfig(queue, uniqueName(t, "delayed"), &config.PartitioningConfig{GlobalConcurrency: 1}), redisURL)
	broker.SetRegisteredTaskNames([]string{"add"})

	client := rawRedisClient(redisURL)
	defer client.Close()
	leaseKey := testPartitionLeaseKey(queue, "A")
	defer client.Del(ctxBg(), queue, testPartitionListKey(queue, "A"), leaseKey, testPartitionsSetKey(queue))

	// Simulate a worker that died while holding a global concurrency slot:
	// a lease entry whose expiry is already in the past.
	require.NoError(t, client.ZAdd(ctxBg(), leaseKey, goredis.Z{
		Score:  float64(time.Now().Add(-time.Hour).UnixNano()),
		Member: "dead-task-uuid",
	}).Err())

	processed := make(chan *tasks.Signature, 1)
	processor := &testTaskProcessor{processed: processed}

	done := make(chan struct{})
	go func() {
		broker.StartConsuming("tag", 1, processor)
		close(done)
	}()

	require.NoError(t, broker.Publish(context.Background(), &tasks.Signature{UUID: "fresh", Name: "add", PartitionKey: "A"}))

	select {
	case got := <-processed:
		assert.Equal(t, "fresh", got.UUID)
	case <-time.After(3 * time.Second):
		t.Fatal("stale lease entry blocked a fresh task from acquiring its partition's global concurrency slot")
	}

	broker.StopConsuming()
	<-done
}

func TestStartConsumingPartitionedNotRegisteredRequeued(t *testing.T) {
	redisURL := getRedisURL(t)
	queue := uniqueName(t, "queue")
	broker := newBrokerWithConfig(t, partitionedConfig(queue, uniqueName(t, "delayed"), &config.PartitioningConfig{}), redisURL)
	// Nothing registered, and IgnoreWhenTaskNotRegistered defaults to false.

	client := rawRedisClient(redisURL)
	defer client.Close()
	defer client.Del(ctxBg(), queue, testPartitionListKey(queue, "A"), testPartitionsSetKey(queue))

	processor := &testTaskProcessor{}

	done := make(chan struct{})
	go func() {
		broker.StartConsuming("tag", 1, processor)
		close(done)
	}()

	sig := &tasks.Signature{UUID: "task-requeue", Name: "unknown", PartitionKey: "A"}
	require.NoError(t, broker.Publish(context.Background(), sig))

	time.Sleep(150 * time.Millisecond)
	broker.StopConsuming()
	<-done

	count, err := client.LLen(ctxBg(), testPartitionListKey(queue, "A")).Result()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, count, int64(1))
}
