package result_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1/backends/eager"
	"github.com/RichardKnop/machinery/v1/backends/iface"
	"github.com/RichardKnop/machinery/v1/backends/result"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func successResults() []*tasks.TaskResult {
	return []*tasks.TaskResult{
		{Type: "int64", Value: json.Number("300")},
	}
}

// pendingThenBackend wraps a real backend and reports a task as still
// PENDING for a configured number of GetState calls before delegating to
// the wrapped backend. This lets tests deterministically exercise the
// polling/sleep loops in Get/GetWithTimeout without racing a background
// goroutine against the eager backend's unsynchronized reads.
type pendingThenBackend struct {
	iface.Backend
	pendingLeft map[string]int
}

func newPendingThenBackend(backend iface.Backend, pendingLeft map[string]int) *pendingThenBackend {
	return &pendingThenBackend{Backend: backend, pendingLeft: pendingLeft}
}

func (b *pendingThenBackend) GetState(ctx context.Context, taskUUID string) (*tasks.TaskState, error) {
	if remaining, ok := b.pendingLeft[taskUUID]; ok && remaining > 0 {
		b.pendingLeft[taskUUID] = remaining - 1
		return &tasks.TaskState{TaskUUID: taskUUID, State: tasks.StatePending}, nil
	}
	return b.Backend.GetState(ctx, taskUUID)
}

func TestAsyncResult_Touch_BackendNotConfigured(t *testing.T) {
	asyncResult := result.NewAsyncResult(&tasks.Signature{UUID: "1"}, nil)

	results, err := asyncResult.Touch(context.Background())
	assert.Nil(t, results)
	assert.Equal(t, result.ErrBackendNotConfigured, err)
}

func TestAsyncResult_Touch_Pending(t *testing.T) {
	backend := eager.New()
	signature := &tasks.Signature{UUID: "1"}
	require.NoError(t, backend.SetStatePending(context.Background(), signature))

	asyncResult := result.NewAsyncResult(signature, backend)
	results, err := asyncResult.Touch(context.Background())
	assert.Nil(t, results)
	assert.NoError(t, err)
}

func TestAsyncResult_Touch_Failure(t *testing.T) {
	backend := eager.New()
	signature := &tasks.Signature{UUID: "1"}
	require.NoError(t, backend.SetStateFailure(context.Background(), signature, "something went wrong"))

	asyncResult := result.NewAsyncResult(signature, backend)
	results, err := asyncResult.Touch(context.Background())
	assert.Nil(t, results)
	assert.EqualError(t, err, "something went wrong")
}

func TestAsyncResult_Touch_Success(t *testing.T) {
	backend := eager.New()
	signature := &tasks.Signature{UUID: "1"}
	require.NoError(t, backend.SetStateSuccess(context.Background(), signature, successResults()))

	asyncResult := result.NewAsyncResult(signature, backend)
	results, err := asyncResult.Touch(context.Background())
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, int64(300), results[0].Interface())
}

func TestAsyncResult_Get(t *testing.T) {
	inner := eager.New()
	signature := &tasks.Signature{UUID: "1"}
	require.NoError(t, inner.SetStateSuccess(context.Background(), signature, successResults()))

	backend := newPendingThenBackend(inner, map[string]int{"1": 2})
	asyncResult := result.NewAsyncResult(signature, backend)

	results, err := asyncResult.Get(context.Background(), time.Millisecond)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, int64(300), results[0].Interface())
}

func TestAsyncResult_GetWithTimeout_Success(t *testing.T) {
	inner := eager.New()
	signature := &tasks.Signature{UUID: "1"}
	require.NoError(t, inner.SetStateSuccess(context.Background(), signature, successResults()))

	backend := newPendingThenBackend(inner, map[string]int{"1": 2})
	asyncResult := result.NewAsyncResult(signature, backend)

	results, err := asyncResult.GetWithTimeout(context.Background(), 100*time.Millisecond, time.Millisecond)
	require.NoError(t, err)
	require.Len(t, results, 1)
}

func TestAsyncResult_GetWithTimeout_TimeoutReached(t *testing.T) {
	backend := eager.New()
	signature := &tasks.Signature{UUID: "1"}
	require.NoError(t, backend.SetStatePending(context.Background(), signature))

	asyncResult := result.NewAsyncResult(signature, backend)
	results, err := asyncResult.GetWithTimeout(context.Background(), 5*time.Millisecond, 20*time.Millisecond)
	assert.Nil(t, results)
	assert.Equal(t, result.ErrTimeoutReached, err)
}

func TestAsyncResult_GetState_CachesCompletedState(t *testing.T) {
	backend := eager.New()
	signature := &tasks.Signature{UUID: "1"}
	require.NoError(t, backend.SetStateSuccess(context.Background(), signature, successResults()))

	asyncResult := result.NewAsyncResult(signature, backend)
	first := asyncResult.GetState(context.Background())
	require.NotNil(t, first)
	assert.True(t, first.IsSuccess())

	require.NoError(t, backend.PurgeState(context.Background(), signature.UUID))

	second := asyncResult.GetState(context.Background())
	assert.Same(t, first, second)
}

func TestAsyncResult_GetState_BackendError(t *testing.T) {
	backend := eager.New()
	signature := &tasks.Signature{UUID: "missing"}

	asyncResult := result.NewAsyncResult(signature, backend)
	state := asyncResult.GetState(context.Background())
	require.NotNil(t, state)
	assert.Empty(t, state.State)
}

func newChainSignatures(uuids ...string) []*tasks.Signature {
	signatures := make([]*tasks.Signature, len(uuids))
	for i, uuid := range uuids {
		signatures[i] = &tasks.Signature{UUID: uuid}
	}
	return signatures
}

func TestChainAsyncResult_Get_BackendNotConfigured(t *testing.T) {
	chainResult := result.NewChainAsyncResult(newChainSignatures("1", "2"), nil)
	results, err := chainResult.Get(context.Background(), time.Millisecond)
	assert.Nil(t, results)
	assert.Equal(t, result.ErrBackendNotConfigured, err)
}

func TestChainAsyncResult_Get_Success(t *testing.T) {
	backend := eager.New()
	signatures := newChainSignatures("1", "2", "3")
	for _, s := range signatures {
		require.NoError(t, backend.SetStateSuccess(context.Background(), s, successResults()))
	}

	chainResult := result.NewChainAsyncResult(signatures, backend)
	results, err := chainResult.Get(context.Background(), time.Millisecond)
	require.NoError(t, err)
	require.Len(t, results, 1)
}

func TestChainAsyncResult_Get_Failure(t *testing.T) {
	backend := eager.New()
	signatures := newChainSignatures("1", "2")
	require.NoError(t, backend.SetStateSuccess(context.Background(), signatures[0], successResults()))
	require.NoError(t, backend.SetStateFailure(context.Background(), signatures[1], "boom"))

	chainResult := result.NewChainAsyncResult(signatures, backend)
	results, err := chainResult.Get(context.Background(), time.Millisecond)
	assert.Nil(t, results)
	assert.EqualError(t, err, "boom")
}

func TestChainAsyncResult_GetWithTimeout_BackendNotConfigured(t *testing.T) {
	chainResult := result.NewChainAsyncResult(newChainSignatures("1"), nil)
	results, err := chainResult.GetWithTimeout(context.Background(), time.Millisecond, time.Millisecond)
	assert.Nil(t, results)
	assert.Equal(t, result.ErrBackendNotConfigured, err)
}

func TestChainAsyncResult_GetWithTimeout_Success(t *testing.T) {
	inner := eager.New()
	signatures := newChainSignatures("1", "2")
	require.NoError(t, inner.SetStateSuccess(context.Background(), signatures[0], successResults()))
	require.NoError(t, inner.SetStateSuccess(context.Background(), signatures[1], successResults()))

	// signature "2" is both the last signature and the one re-touched
	// directly, so it is read twice per polling round; 3 pending reads
	// therefore spans two rounds before it resolves to success.
	backend := newPendingThenBackend(inner, map[string]int{"2": 3})
	chainResult := result.NewChainAsyncResult(signatures, backend)

	results, err := chainResult.GetWithTimeout(context.Background(), 200*time.Millisecond, time.Millisecond)
	require.NoError(t, err)
	require.Len(t, results, 1)
}

func TestChainAsyncResult_GetWithTimeout_EarlyFailure(t *testing.T) {
	backend := eager.New()
	signatures := newChainSignatures("1", "2")
	require.NoError(t, backend.SetStateFailure(context.Background(), signatures[0], "early failure"))
	require.NoError(t, backend.SetStatePending(context.Background(), signatures[1]))

	chainResult := result.NewChainAsyncResult(signatures, backend)
	results, err := chainResult.GetWithTimeout(context.Background(), 200*time.Millisecond, 2*time.Millisecond)
	assert.Nil(t, results)
	assert.EqualError(t, err, "early failure")
}

func TestChainAsyncResult_GetWithTimeout_LastFailure(t *testing.T) {
	backend := eager.New()
	signatures := newChainSignatures("1", "2")
	require.NoError(t, backend.SetStateSuccess(context.Background(), signatures[0], successResults()))
	require.NoError(t, backend.SetStateFailure(context.Background(), signatures[1], "last failure"))

	chainResult := result.NewChainAsyncResult(signatures, backend)
	results, err := chainResult.GetWithTimeout(context.Background(), 200*time.Millisecond, 2*time.Millisecond)
	assert.Nil(t, results)
	assert.EqualError(t, err, "last failure")
}

func TestChainAsyncResult_GetWithTimeout_TimeoutReached(t *testing.T) {
	backend := eager.New()
	signatures := newChainSignatures("1")
	require.NoError(t, backend.SetStatePending(context.Background(), signatures[0]))

	chainResult := result.NewChainAsyncResult(signatures, backend)
	results, err := chainResult.GetWithTimeout(context.Background(), 5*time.Millisecond, 20*time.Millisecond)
	assert.Nil(t, results)
	assert.Equal(t, result.ErrTimeoutReached, err)
}

func newChordAsyncResult(backend iface.Backend, groupUUIDs []string, callbackUUID string) *result.ChordAsyncResult {
	group := make([]*tasks.Signature, len(groupUUIDs))
	for i, uuid := range groupUUIDs {
		group[i] = &tasks.Signature{UUID: uuid}
	}
	callback := &tasks.Signature{UUID: callbackUUID}
	return result.NewChordAsyncResult(group, callback, backend)
}

func TestChordAsyncResult_Get_BackendNotConfigured(t *testing.T) {
	chordResult := newChordAsyncResult(nil, []string{"1"}, "cb")
	results, err := chordResult.Get(context.Background(), time.Millisecond)
	assert.Nil(t, results)
	assert.Equal(t, result.ErrBackendNotConfigured, err)
}

func TestChordAsyncResult_Get_Success(t *testing.T) {
	backend := eager.New()
	require.NoError(t, backend.SetStateSuccess(context.Background(), &tasks.Signature{UUID: "1"}, successResults()))
	require.NoError(t, backend.SetStateSuccess(context.Background(), &tasks.Signature{UUID: "2"}, successResults()))
	require.NoError(t, backend.SetStateSuccess(context.Background(), &tasks.Signature{UUID: "cb"}, successResults()))

	chordResult := newChordAsyncResult(backend, []string{"1", "2"}, "cb")
	results, err := chordResult.Get(context.Background(), time.Millisecond)
	require.NoError(t, err)
	require.Len(t, results, 1)
}

func TestChordAsyncResult_Get_GroupFailure(t *testing.T) {
	backend := eager.New()
	require.NoError(t, backend.SetStateFailure(context.Background(), &tasks.Signature{UUID: "1"}, "group task failed"))

	chordResult := newChordAsyncResult(backend, []string{"1"}, "cb")
	results, err := chordResult.Get(context.Background(), time.Millisecond)
	assert.Nil(t, results)
	assert.EqualError(t, err, "group task failed")
}

func TestChordAsyncResult_GetWithTimeout_BackendNotConfigured(t *testing.T) {
	chordResult := newChordAsyncResult(nil, []string{"1"}, "cb")
	results, err := chordResult.GetWithTimeout(context.Background(), time.Millisecond, time.Millisecond)
	assert.Nil(t, results)
	assert.Equal(t, result.ErrBackendNotConfigured, err)
}

func TestChordAsyncResult_GetWithTimeout_Success(t *testing.T) {
	inner := eager.New()
	require.NoError(t, inner.SetStateSuccess(context.Background(), &tasks.Signature{UUID: "1"}, successResults()))
	require.NoError(t, inner.SetStateSuccess(context.Background(), &tasks.Signature{UUID: "cb"}, successResults()))

	backend := newPendingThenBackend(inner, map[string]int{"cb": 1})
	chordResult := newChordAsyncResult(backend, []string{"1"}, "cb")

	results, err := chordResult.GetWithTimeout(context.Background(), 200*time.Millisecond, time.Millisecond)
	require.NoError(t, err)
	require.Len(t, results, 1)
}

func TestChordAsyncResult_GetWithTimeout_GroupFailure(t *testing.T) {
	backend := eager.New()
	require.NoError(t, backend.SetStateFailure(context.Background(), &tasks.Signature{UUID: "1"}, "group task failed"))
	require.NoError(t, backend.SetStatePending(context.Background(), &tasks.Signature{UUID: "cb"}))

	chordResult := newChordAsyncResult(backend, []string{"1"}, "cb")
	// NOTE: production code returns (nil, err) here using the outer `err`
	// variable from a previous loop iteration rather than the error from
	// this failed Touch() call, so on the very first iteration this
	// actually yields (nil, nil). This test documents that existing
	// behavior rather than the apparently-intended one.
	results, err := chordResult.GetWithTimeout(context.Background(), 200*time.Millisecond, 2*time.Millisecond)
	assert.Nil(t, results)
	assert.NoError(t, err)
}

func TestChordAsyncResult_GetWithTimeout_CallbackFailure(t *testing.T) {
	backend := eager.New()
	require.NoError(t, backend.SetStateSuccess(context.Background(), &tasks.Signature{UUID: "1"}, successResults()))
	require.NoError(t, backend.SetStateFailure(context.Background(), &tasks.Signature{UUID: "cb"}, "callback failed"))

	chordResult := newChordAsyncResult(backend, []string{"1"}, "cb")
	// NOTE: production code swallows the callback error and returns
	// (nil, nil) instead of (nil, err) here. This test documents that
	// existing behavior.
	results, err := chordResult.GetWithTimeout(context.Background(), 200*time.Millisecond, 2*time.Millisecond)
	assert.Nil(t, results)
	assert.NoError(t, err)
}

func TestChordAsyncResult_GetWithTimeout_TimeoutReached(t *testing.T) {
	backend := eager.New()
	require.NoError(t, backend.SetStatePending(context.Background(), &tasks.Signature{UUID: "1"}))
	require.NoError(t, backend.SetStatePending(context.Background(), &tasks.Signature{UUID: "cb"}))

	chordResult := newChordAsyncResult(backend, []string{"1"}, "cb")
	results, err := chordResult.GetWithTimeout(context.Background(), 5*time.Millisecond, 20*time.Millisecond)
	assert.Nil(t, results)
	assert.Equal(t, result.ErrTimeoutReached, err)
}
