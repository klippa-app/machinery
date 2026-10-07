package eager_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/machinery/v1/brokers/eager"
	"github.com/RichardKnop/machinery/v1/tasks"
)

type fakeProcessor struct {
	processFunc func(*tasks.Signature) error
	customQueue string
}

func (p *fakeProcessor) Process(ctx context.Context, sig *tasks.Signature) error {
	if p.processFunc != nil {
		return p.processFunc(sig)
	}
	return nil
}

func (p *fakeProcessor) CustomQueue() string {
	return p.customQueue
}

func (p *fakeProcessor) PreConsumeHandler() bool {
	return true
}

func TestStartConsuming(t *testing.T) {
	broker := eager.New()
	retry, err := broker.StartConsuming("tag", 1, &fakeProcessor{})
	assert.True(t, retry)
	assert.NoError(t, err)
}

func TestStopConsuming(t *testing.T) {
	broker := eager.New()
	assert.NotPanics(t, func() {
		broker.StopConsuming()
	})
}

func TestPublishNoWorkerAssigned(t *testing.T) {
	broker := eager.New()
	err := broker.Publish(context.Background(), &tasks.Signature{UUID: "1"})
	assert.EqualError(t, err, "worker is not assigned in eager-mode")
}

func TestPublishSuccess(t *testing.T) {
	broker := eager.New()
	mode, ok := broker.(eager.Mode)
	require.True(t, ok)

	var processed *tasks.Signature
	mode.AssignWorker(&fakeProcessor{processFunc: func(sig *tasks.Signature) error {
		processed = sig
		return nil
	}})

	err := broker.Publish(context.Background(), &tasks.Signature{UUID: "task1", Name: "add"})
	assert.NoError(t, err)
	require.NotNil(t, processed)
	assert.Equal(t, "task1", processed.UUID)
}

func TestPublishProcessError(t *testing.T) {
	broker := eager.New()
	mode, ok := broker.(eager.Mode)
	require.True(t, ok)

	mode.AssignWorker(&fakeProcessor{processFunc: func(sig *tasks.Signature) error {
		return assert.AnError
	}})

	err := broker.Publish(context.Background(), &tasks.Signature{UUID: "task1"})
	assert.Equal(t, assert.AnError, err)
}

func TestPublishMarshalError(t *testing.T) {
	broker := eager.New()
	mode, ok := broker.(eager.Mode)
	require.True(t, ok)
	mode.AssignWorker(&fakeProcessor{})

	sig := &tasks.Signature{
		UUID: "task1",
		Headers: tasks.Headers{
			"bad": make(chan int),
		},
	}
	err := broker.Publish(context.Background(), sig)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "JSON marshal error")
}
