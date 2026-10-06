package common_test

import (
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
)

func TestNewBroker(t *testing.T) {
	t.Parallel()

	cnf := &config.Config{DefaultQueue: "queue"}
	broker := common.NewBroker(cnf)

	assert.Equal(t, cnf, broker.GetConfig())
	assert.True(t, broker.GetRetry())
	assert.Nil(t, broker.GetRetryFunc())
	assert.NotNil(t, broker.GetRetryStopChan())
	assert.NotNil(t, broker.GetStopChan())
}

func TestBrokerPublish(t *testing.T) {
	t.Parallel()

	broker := common.NewBroker(new(config.Config))
	err := broker.Publish(&tasks.Signature{})
	assert.EqualError(t, err, "Not implemented")
}

func TestGetPendingTasks(t *testing.T) {
	t.Parallel()

	broker := common.NewBroker(new(config.Config))
	pendingTasks, err := broker.GetPendingTasks("queue")
	assert.Nil(t, pendingTasks)
	assert.EqualError(t, err, "Not implemented")
}

func TestGetPendingTaskCount(t *testing.T) {
	t.Parallel()

	broker := common.NewBroker(new(config.Config))
	count, err := broker.GetPendingTaskCount("queue")
	assert.Equal(t, int64(0), count)
	assert.EqualError(t, err, "Not implemented")
}

func TestGetDelayedTasks(t *testing.T) {
	t.Parallel()

	broker := common.NewBroker(new(config.Config))
	delayedTasks, err := broker.GetDelayedTasks()
	assert.Nil(t, delayedTasks)
	assert.EqualError(t, err, "Not implemented")
}

func TestCancelDelayedTask(t *testing.T) {
	t.Parallel()

	broker := common.NewBroker(new(config.Config))
	err := broker.CancelDelayedTask("task-uuid")
	assert.EqualError(t, err, "Not implemented")
}

func TestIsTaskRegistered(t *testing.T) {
	t.Parallel()

	broker := common.NewBroker(new(config.Config))
	broker.SetRegisteredTaskNames([]string{"foo", "bar"})

	assert.True(t, broker.IsTaskRegistered("foo"))
	assert.False(t, broker.IsTaskRegistered("bogus"))
}

func TestAdjustRoutingKey(t *testing.T) {
	t.Parallel()

	var (
		s      *tasks.Signature
		broker common.Broker
	)

	t.Run("with routing key", func(t *testing.T) {
		s = &tasks.Signature{RoutingKey: "routing_key"}
		broker = common.NewBroker(&config.Config{
			DefaultQueue: "queue",
		})
		broker.AdjustRoutingKey(s)
		assert.Equal(t, "routing_key", s.RoutingKey)
	})

	t.Run("without routing key", func(t *testing.T) {
		s = new(tasks.Signature)
		broker = common.NewBroker(&config.Config{
			DefaultQueue: "queue",
		})
		broker.AdjustRoutingKey(s)
		assert.Equal(t, "queue", s.RoutingKey)
	})
}

func TestGetRegisteredTaskNames(t *testing.T) {
	t.Parallel()

	broker := common.NewBroker(new(config.Config))
	fooTasks := []string{"foo", "bar", "baz"}
	broker.SetRegisteredTaskNames(fooTasks)
	assert.Equal(t, fooTasks, broker.GetRegisteredTaskNames())
}

func TestStopConsuming(t *testing.T) {
	t.Parallel()

	t.Run("stop consuming", func(t *testing.T) {
		broker := common.NewBroker(&config.Config{
			DefaultQueue: "queue",
		})
		broker.StartConsuming("", 1, &machinery.Worker{})
		broker.StopConsuming()
		select {
		case <-broker.GetStopChan():
		default:
			assert.Fail(t, "still blocking")
		}
	})

	t.Run("notifies a listening retry closure", func(t *testing.T) {
		broker := common.NewBroker(&config.Config{
			DefaultQueue: "queue",
		})
		broker.StartConsuming("", 1, &machinery.Worker{})

		received := make(chan struct{})
		go func() {
			<-broker.GetRetryStopChan()
			close(received)
		}()
		time.Sleep(50 * time.Millisecond)

		broker.StopConsuming()

		select {
		case <-received:
		case <-time.After(time.Second):
			assert.Fail(t, "retry closure was not notified")
		}
	})
}
