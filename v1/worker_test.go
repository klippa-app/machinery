package machinery_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"

	eagerbackend "github.com/RichardKnop/machinery/v1/backends/eager"
	backendsiface "github.com/RichardKnop/machinery/v1/backends/iface"
	eagerbroker "github.com/RichardKnop/machinery/v1/brokers/eager"
	"github.com/RichardKnop/machinery/v1/brokers/errs"
	brokersiface "github.com/RichardKnop/machinery/v1/brokers/iface"
)

// fakeBroker is a minimal brokersiface.Broker used to exercise worker/server
// code paths (Launch/Quit, retry-on-StartConsuming-error, Publish failures)
// that the eager broker cannot reach, since it always starts consuming
// successfully and always publishes synchronously without error.
type fakeBroker struct {
	cnf *config.Config

	publishErr   error
	publishFunc  func(ctx context.Context, sig *tasks.Signature) error
	publishCount int32

	startFunc func(consumerTag string, concurrency int, p brokersiface.TaskProcessor) (bool, error)
	stopFunc  func()
}

func (b *fakeBroker) GetConfig() *config.Config             { return b.cnf }
func (b *fakeBroker) SetRegisteredTaskNames(names []string) {}
func (b *fakeBroker) IsTaskRegistered(name string) bool     { return true }

func (b *fakeBroker) StartConsuming(consumerTag string, concurrency int, p brokersiface.TaskProcessor) (bool, error) {
	if b.startFunc != nil {
		return b.startFunc(consumerTag, concurrency, p)
	}
	return false, nil
}

func (b *fakeBroker) StopConsuming() {
	if b.stopFunc != nil {
		b.stopFunc()
	}
}

func (b *fakeBroker) Publish(ctx context.Context, task *tasks.Signature) error {
	atomic.AddInt32(&b.publishCount, 1)
	if b.publishFunc != nil {
		return b.publishFunc(ctx, task)
	}
	return b.publishErr
}

func (b *fakeBroker) GetPendingTasks(queue string) ([]*tasks.Signature, error) { return nil, nil }
func (b *fakeBroker) GetPendingTaskCount(queue string) (int64, error)          { return 0, nil }
func (b *fakeBroker) GetDelayedTasks() ([]*tasks.Signature, error)             { return nil, nil }
func (b *fakeBroker) CancelDelayedTask(taskUUID string) error                  { return nil }
func (b *fakeBroker) AdjustRoutingKey(s *tasks.Signature)                      {}

// fakeBackend wraps a real eager backend and lets individual tests inject
// errors from specific methods, to reach the error-wrapping branches in
// worker.go that the eager backend (which never fails) cannot reach.
type fakeBackend struct {
	backendsiface.Backend

	onSetStatePending  func(*tasks.Signature) error
	onSetStateReceived func(*tasks.Signature) error
	onSetStateStarted  func(*tasks.Signature) error
	onSetStateRetry    func(*tasks.Signature) error
	onSetStateSuccess  func(*tasks.Signature, []*tasks.TaskResult) error
	onSetStateFailure  func(*tasks.Signature, string) error
	onGroupCompleted   func(string, int) (bool, error)
	onTriggerChord     func(string) (bool, error)
	onGroupTaskStates  func(string, int) ([]*tasks.TaskState, error)
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{Backend: eagerbackend.New()}
}

func (b *fakeBackend) SetStatePending(s *tasks.Signature) error {
	if b.onSetStatePending != nil {
		return b.onSetStatePending(s)
	}
	return b.Backend.SetStatePending(s)
}

func (b *fakeBackend) SetStateReceived(s *tasks.Signature) error {
	if b.onSetStateReceived != nil {
		return b.onSetStateReceived(s)
	}
	return b.Backend.SetStateReceived(s)
}

func (b *fakeBackend) SetStateStarted(s *tasks.Signature) error {
	if b.onSetStateStarted != nil {
		return b.onSetStateStarted(s)
	}
	return b.Backend.SetStateStarted(s)
}

func (b *fakeBackend) SetStateRetry(s *tasks.Signature) error {
	if b.onSetStateRetry != nil {
		return b.onSetStateRetry(s)
	}
	return b.Backend.SetStateRetry(s)
}

func (b *fakeBackend) SetStateSuccess(s *tasks.Signature, results []*tasks.TaskResult) error {
	if b.onSetStateSuccess != nil {
		return b.onSetStateSuccess(s, results)
	}
	return b.Backend.SetStateSuccess(s, results)
}

func (b *fakeBackend) SetStateFailure(s *tasks.Signature, errStr string) error {
	if b.onSetStateFailure != nil {
		return b.onSetStateFailure(s, errStr)
	}
	return b.Backend.SetStateFailure(s, errStr)
}

func (b *fakeBackend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	if b.onGroupCompleted != nil {
		return b.onGroupCompleted(groupUUID, groupTaskCount)
	}
	return b.Backend.GroupCompleted(groupUUID, groupTaskCount)
}

func (b *fakeBackend) TriggerChord(groupUUID string) (bool, error) {
	if b.onTriggerChord != nil {
		return b.onTriggerChord(groupUUID)
	}
	return b.Backend.TriggerChord(groupUUID)
}

func (b *fakeBackend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error) {
	if b.onGroupTaskStates != nil {
		return b.onGroupTaskStates(groupUUID, groupTaskCount)
	}
	return b.Backend.GroupTaskStates(groupUUID, groupTaskCount)
}

// newEagerServerWithBackend is like newEagerServerAndWorker but lets the
// caller supply a (possibly faulty) backend implementation.
func newEagerServerWithBackend(t *testing.T, backend backendsiface.Backend) (*machinery.Server, *machinery.Worker) {
	t.Helper()

	cnf := &config.Config{
		Broker:        "eager",
		DefaultQueue:  "machinery_tasks",
		ResultBackend: "eager",
		Lock:          "eager",
	}

	broker, err := machinery.BrokerFactory(cnf)
	require.NoError(t, err)
	lock, err := machinery.LockFactory(cnf)
	require.NoError(t, err)

	server := machinery.NewServerWithBrokerBackendLock(cnf, broker, backend, lock)
	worker := server.NewWorker("test_worker", 0)

	eagerMode, ok := broker.(eagerbroker.Mode)
	require.True(t, ok)
	eagerMode.AssignWorker(worker)

	return server, worker
}

func TestRedactURL(t *testing.T) {
	t.Parallel()

	broker := "amqp://guest:guest@localhost:5672"
	redactedURL := machinery.RedactURL(broker)
	assert.Equal(t, "amqp://localhost:5672", redactedURL)
}

func TestRedactURLInvalidURL(t *testing.T) {
	t.Parallel()

	malformed := "redis://%zz"
	assert.Equal(t, malformed, machinery.RedactURL(malformed))
}

func TestPreConsumeHandler(t *testing.T) {
	t.Parallel()

	worker := &machinery.Worker{}

	worker.SetPreConsumeHandler(SamplePreConsumeHandler)
	assert.True(t, worker.PreConsumeHandler())
}

func SamplePreConsumeHandler(w *machinery.Worker) bool {
	return true
}

func TestPreConsumeHandlerDefault(t *testing.T) {
	t.Parallel()

	worker := &machinery.Worker{}
	assert.True(t, worker.PreConsumeHandler())
}

func TestWorkerGetServer(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	worker := server.NewWorker("test_worker", 1)
	assert.Same(t, server, worker.GetServer())
}

func TestWorkerCustomQueue(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	worker := server.NewCustomQueueWorker("test_worker", 1, "my_custom_queue")
	assert.Equal(t, "my_custom_queue", worker.CustomQueue())

	defaultWorker := server.NewWorker("test_worker", 1)
	assert.Equal(t, "", defaultWorker.CustomQueue())
}

func TestWorkerProcessUnregisteredTask(t *testing.T) {
	t.Parallel()

	_, worker := newEagerServerAndWorker(t)

	sig, err := tasks.NewSignature("not_registered", nil)
	require.NoError(t, err)

	assert.NoError(t, worker.Process(sig))
}

func TestWorkerProcessSuccess(t *testing.T) {
	t.Parallel()

	server, worker := newEagerServerAndWorker(t)
	require.NoError(t, server.RegisterTask("add", func(a, b int64) (int64, error) {
		return a + b, nil
	}))

	sig, err := tasks.NewSignature("add", []tasks.Arg{
		{Type: "int64", Value: int64(2)},
		{Type: "int64", Value: int64(3)},
	})
	require.NoError(t, err)

	require.NoError(t, worker.Process(sig))

	state, err := server.GetBackend().GetState(sig.UUID)
	require.NoError(t, err)
	require.True(t, state.IsSuccess())
	require.Len(t, state.Results, 1)
	// the eager backend round-trips the state through JSON, so numeric
	// results come back as json.Number rather than their original Go type.
	assert.Equal(t, "5", fmt.Sprint(state.Results[0].Value))
}

func TestWorkerProcessMalformedSignature(t *testing.T) {
	t.Parallel()

	server, worker := newEagerServerAndWorker(t)
	require.NoError(t, server.RegisterTask("needs_int", func(a int64) error { return nil }))

	sig, err := tasks.NewSignature("needs_int", []tasks.Arg{{Type: "not_a_real_type", Value: "x"}})
	require.NoError(t, err)

	err = worker.Process(sig)
	assert.Error(t, err)

	state, err2 := server.GetBackend().GetState(sig.UUID)
	require.NoError(t, err2)
	assert.True(t, state.IsFailure())
}

func TestWorkerProcessTaskErrorNoRetry(t *testing.T) {
	t.Parallel()

	server, worker := newEagerServerAndWorker(t)
	require.NoError(t, server.RegisterTask("failing_task", func() error {
		return errors.New("boom")
	}))

	sig, err := tasks.NewSignature("failing_task", nil)
	require.NoError(t, err)

	require.NoError(t, worker.Process(sig))

	state, err2 := server.GetBackend().GetState(sig.UUID)
	require.NoError(t, err2)
	assert.True(t, state.IsFailure())
	assert.Equal(t, "boom", state.Error)
}

func TestWorkerProcessTaskErrorWithRetry(t *testing.T) {
	t.Parallel()

	server, worker := newEagerServerAndWorker(t)
	var attempts int32
	require.NoError(t, server.RegisterTask("flaky_task", func() error {
		atomic.AddInt32(&attempts, 1)
		return errors.New("still broken")
	}))

	sig, err := tasks.NewSignature("flaky_task", nil)
	require.NoError(t, err)
	sig.RetryCount = 1

	require.NoError(t, worker.Process(sig))

	assert.EqualValues(t, 2, atomic.LoadInt32(&attempts))

	state, err2 := server.GetBackend().GetState(sig.UUID)
	require.NoError(t, err2)
	assert.True(t, state.IsFailure())
}

func TestWorkerProcessTaskErrorWithCustomTimeoutFunc(t *testing.T) {
	t.Parallel()

	server, worker := newEagerServerAndWorker(t)
	var usedCustomTimeout int32
	worker.SetTimeoutFunc(func(s *tasks.Signature) int {
		atomic.AddInt32(&usedCustomTimeout, 1)
		return 0
	})

	require.NoError(t, server.RegisterTask("flaky_task", func() error {
		return errors.New("still broken")
	}))

	sig, err := tasks.NewSignature("flaky_task", nil)
	require.NoError(t, err)
	sig.RetryCount = 1

	require.NoError(t, worker.Process(sig))
	assert.EqualValues(t, 1, atomic.LoadInt32(&usedCustomTimeout))
}

func TestWorkerProcessRetryTaskLater(t *testing.T) {
	t.Parallel()

	server, worker := newEagerServerAndWorker(t)
	require.NoError(t, server.RegisterTask("retry_later_task", func() error {
		return tasks.NewErrRetryTaskLater("not yet", time.Millisecond)
	}))

	sig, err := tasks.NewSignature("retry_later_task", nil)
	require.NoError(t, err)
	sig.RetryCount = 2

	require.NoError(t, worker.Process(sig))

	state, err2 := server.GetBackend().GetState(sig.UUID)
	require.NoError(t, err2)
	assert.True(t, state.IsFailure())
	assert.Equal(t, "Task failed too many times.", state.Error)
}

func TestWorkerProcessPreAndPostTaskHandlers(t *testing.T) {
	t.Parallel()

	server, worker := newEagerServerAndWorker(t)
	var preCalls, postCalls int32
	worker.SetPreTaskHandler(func(s *tasks.Signature) { atomic.AddInt32(&preCalls, 1) })
	worker.SetPostTaskHandler(func(s *tasks.Signature) { atomic.AddInt32(&postCalls, 1) })

	require.NoError(t, server.RegisterTask("test_task", func() error { return nil }))

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)

	require.NoError(t, worker.Process(sig))
	assert.EqualValues(t, 1, atomic.LoadInt32(&preCalls))
	assert.EqualValues(t, 1, atomic.LoadInt32(&postCalls))
}

func TestWorkerProcessOnSuccessCallback(t *testing.T) {
	t.Parallel()

	server, worker := newEagerServerAndWorker(t)
	var gotValue int64
	var called int32
	require.NoError(t, server.RegisterTask("main_task", func() (int64, error) { return 42, nil }))
	require.NoError(t, server.RegisterTask("success_cb", func(v int64) error {
		atomic.AddInt32(&called, 1)
		gotValue = v
		return nil
	}))

	cb, err := tasks.NewSignature("success_cb", nil)
	require.NoError(t, err)
	sig, err := tasks.NewSignature("main_task", nil)
	require.NoError(t, err)
	sig.OnSuccess = []*tasks.Signature{cb}

	require.NoError(t, worker.Process(sig))
	assert.EqualValues(t, 1, atomic.LoadInt32(&called))
	assert.Equal(t, int64(42), gotValue)
}

func TestWorkerProcessOnErrorCallback(t *testing.T) {
	t.Parallel()

	server, worker := newEagerServerAndWorker(t)
	var gotMessage string
	require.NoError(t, server.RegisterTask("failing_task2", func() error {
		return errors.New("kaboom")
	}))
	require.NoError(t, server.RegisterTask("error_cb", func(msg string) error {
		gotMessage = msg
		return nil
	}))

	cb, err := tasks.NewSignature("error_cb", nil)
	require.NoError(t, err)
	sig, err := tasks.NewSignature("failing_task2", nil)
	require.NoError(t, err)
	sig.OnError = []*tasks.Signature{cb}

	require.NoError(t, worker.Process(sig))
	assert.Equal(t, "kaboom", gotMessage)
}

func TestWorkerProcessStopTaskDeletionOnError(t *testing.T) {
	t.Parallel()

	server, worker := newEagerServerAndWorker(t)
	require.NoError(t, server.RegisterTask("stop_delete_task", func() error {
		return errors.New("fail")
	}))

	sig, err := tasks.NewSignature("stop_delete_task", nil)
	require.NoError(t, err)
	sig.StopTaskDeletionOnError = true

	err = worker.Process(sig)
	assert.ErrorIs(t, err, errs.ErrStopTaskDeletion)
}

func TestWorkerProcessSetStateReceivedError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("received failed")
	backend := newFakeBackend()
	backend.onSetStateReceived = func(s *tasks.Signature) error { return wantErr }

	server, worker := newEagerServerWithBackend(t, backend)
	require.NoError(t, server.RegisterTask("test_task", func() error { return nil }))

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)

	err = worker.Process(sig)
	assert.ErrorContains(t, err, "received failed")
}

func TestWorkerProcessSetStateStartedError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("started failed")
	backend := newFakeBackend()
	backend.onSetStateStarted = func(s *tasks.Signature) error { return wantErr }

	server, worker := newEagerServerWithBackend(t, backend)
	require.NoError(t, server.RegisterTask("test_task", func() error { return nil }))

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)

	err = worker.Process(sig)
	assert.ErrorContains(t, err, "started failed")
}

func TestWorkerProcessSetStateRetryErrorInTaskRetry(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("retry state failed")
	backend := newFakeBackend()
	backend.onSetStateRetry = func(s *tasks.Signature) error { return wantErr }

	server, worker := newEagerServerWithBackend(t, backend)
	require.NoError(t, server.RegisterTask("flaky_task", func() error {
		return errors.New("still broken")
	}))

	sig, err := tasks.NewSignature("flaky_task", nil)
	require.NoError(t, err)
	sig.RetryCount = 1

	err = worker.Process(sig)
	assert.ErrorContains(t, err, "retry state failed")
}

func TestWorkerProcessSetStateRetryErrorInRetryTaskIn(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("retry state failed")
	backend := newFakeBackend()
	backend.onSetStateRetry = func(s *tasks.Signature) error { return wantErr }

	server, worker := newEagerServerWithBackend(t, backend)
	require.NoError(t, server.RegisterTask("retry_later_task", func() error {
		return tasks.NewErrRetryTaskLater("not yet", time.Millisecond)
	}))

	sig, err := tasks.NewSignature("retry_later_task", nil)
	require.NoError(t, err)
	sig.RetryCount = 2

	err = worker.Process(sig)
	assert.ErrorContains(t, err, "retry state failed")
}

func TestWorkerProcessSetStateSuccessError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("success state failed")
	backend := newFakeBackend()
	backend.onSetStateSuccess = func(s *tasks.Signature, r []*tasks.TaskResult) error { return wantErr }

	server, worker := newEagerServerWithBackend(t, backend)
	require.NoError(t, server.RegisterTask("test_task", func() error { return nil }))

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)

	err = worker.Process(sig)
	assert.ErrorContains(t, err, "success state failed")
}

func TestWorkerProcessSetStateFailureError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("failure state failed")
	backend := newFakeBackend()
	backend.onSetStateFailure = func(s *tasks.Signature, errStr string) error { return wantErr }

	server, worker := newEagerServerWithBackend(t, backend)
	require.NoError(t, server.RegisterTask("failing_task", func() error {
		return errors.New("boom")
	}))

	sig, err := tasks.NewSignature("failing_task", nil)
	require.NoError(t, err)

	err = worker.Process(sig)
	assert.ErrorContains(t, err, "failure state failed")
}

func TestWorkerProcessGroupCompletedError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("group completed check failed")
	backend := newFakeBackend()
	backend.onGroupCompleted = func(groupUUID string, count int) (bool, error) { return false, wantErr }

	server, worker := newEagerServerWithBackend(t, backend)
	require.NoError(t, server.RegisterTask("test_task", func() error { return nil }))

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)
	sig.GroupUUID = "some_group"
	sig.GroupTaskCount = 1
	sig.ChordCallback, err = tasks.NewSignature("test_task", nil)
	require.NoError(t, err)

	err = worker.Process(sig)
	assert.ErrorContains(t, err, "group completed check failed")
}

func TestWorkerProcessGroupNotCompletedYet(t *testing.T) {
	t.Parallel()

	backend := newFakeBackend()
	backend.onGroupCompleted = func(groupUUID string, count int) (bool, error) { return false, nil }

	server, worker := newEagerServerWithBackend(t, backend)
	require.NoError(t, server.RegisterTask("test_task", func() error { return nil }))

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)
	sig.GroupUUID = "some_group"
	sig.GroupTaskCount = 2
	sig.ChordCallback, err = tasks.NewSignature("test_task", nil)
	require.NoError(t, err)

	assert.NoError(t, worker.Process(sig))
}

func TestWorkerProcessTriggerChordError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("trigger chord failed")
	backend := newFakeBackend()
	backend.onGroupCompleted = func(groupUUID string, count int) (bool, error) { return true, nil }
	backend.onTriggerChord = func(groupUUID string) (bool, error) { return false, wantErr }

	server, worker := newEagerServerWithBackend(t, backend)
	require.NoError(t, server.RegisterTask("test_task", func() error { return nil }))

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)
	sig.GroupUUID = "some_group"
	sig.GroupTaskCount = 1
	sig.ChordCallback, err = tasks.NewSignature("test_task", nil)
	require.NoError(t, err)

	err = worker.Process(sig)
	assert.ErrorContains(t, err, "trigger chord failed")
}

func TestWorkerProcessChordAlreadyTriggered(t *testing.T) {
	t.Parallel()

	backend := newFakeBackend()
	backend.onGroupCompleted = func(groupUUID string, count int) (bool, error) { return true, nil }
	backend.onTriggerChord = func(groupUUID string) (bool, error) { return false, nil }

	server, worker := newEagerServerWithBackend(t, backend)
	require.NoError(t, server.RegisterTask("test_task", func() error { return nil }))

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)
	sig.GroupUUID = "some_group"
	sig.GroupTaskCount = 1
	sig.ChordCallback, err = tasks.NewSignature("test_task", nil)
	require.NoError(t, err)

	assert.NoError(t, worker.Process(sig))
}

func TestWorkerProcessGroupTaskStatesError(t *testing.T) {
	t.Parallel()

	backend := newFakeBackend()
	backend.onGroupCompleted = func(groupUUID string, count int) (bool, error) { return true, nil }
	backend.onTriggerChord = func(groupUUID string) (bool, error) { return true, nil }
	backend.onGroupTaskStates = func(groupUUID string, count int) ([]*tasks.TaskState, error) {
		return nil, errors.New("states lookup failed")
	}

	server, worker := newEagerServerWithBackend(t, backend)
	require.NoError(t, server.RegisterTask("test_task", func() error { return nil }))

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)
	sig.GroupUUID = "some_group"
	sig.GroupTaskCount = 1
	sig.ChordCallback, err = tasks.NewSignature("test_task", nil)
	require.NoError(t, err)

	// GroupTaskStates errors are only logged, not propagated, so the task is
	// still considered processed successfully from the caller's perspective.
	assert.NoError(t, worker.Process(sig))
}

func TestWorkerProcessGroupTaskNotSuccessfulSkipsChordCallback(t *testing.T) {
	t.Parallel()

	backend := newFakeBackend()
	backend.onGroupCompleted = func(groupUUID string, count int) (bool, error) { return true, nil }
	backend.onTriggerChord = func(groupUUID string) (bool, error) { return true, nil }
	backend.onGroupTaskStates = func(groupUUID string, count int) ([]*tasks.TaskState, error) {
		return []*tasks.TaskState{
			{TaskUUID: "other_task", State: tasks.StateFailure, Error: "sibling failed"},
		}, nil
	}

	server, worker := newEagerServerWithBackend(t, backend)
	require.NoError(t, server.RegisterTask("test_task", func() error { return nil }))

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)
	sig.GroupUUID = "some_group"
	sig.GroupTaskCount = 1
	sig.ChordCallback, err = tasks.NewSignature("test_task", nil)
	require.NoError(t, err)

	assert.NoError(t, worker.Process(sig))
}

func TestWorkerProcessGroupTaskStatesSuccessSendsChordCallback(t *testing.T) {
	t.Parallel()

	// Unlike the other chord-related tests above (which force GroupCompleted/
	// TriggerChord/GroupTaskStates to isolate individual branches), this one
	// uses the real eager backend's GroupTaskStates so it exercises the final
	// "all tasks succeeded, send the chord callback" branch of taskSucceeded.
	backend := newFakeBackend()
	backend.onGroupCompleted = func(groupUUID string, count int) (bool, error) { return true, nil }
	backend.onTriggerChord = func(groupUUID string) (bool, error) { return true, nil }

	server, worker := newEagerServerWithBackend(t, backend)
	require.NoError(t, server.RegisterTask("test_task", func() error { return nil }))

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)
	sig.GroupUUID = "some_group"
	sig.GroupTaskCount = 1
	sig.ChordCallback, err = tasks.NewSignature("test_task", nil)
	require.NoError(t, err)

	require.NoError(t, backend.Backend.InitGroup(sig.GroupUUID, []string{sig.UUID}))

	require.NoError(t, worker.Process(sig))

	callbackState, err := backend.Backend.GetState(sig.ChordCallback.UUID)
	require.NoError(t, err)
	assert.True(t, callbackState.IsSuccess())
}

func TestWorkerProcessUnreflectableResultLogsWarningButSucceeds(t *testing.T) {
	t.Parallel()

	server, worker := newEagerServerAndWorker(t)
	// map[string]string is not one of the types tasks.ReflectValue understands,
	// so tasks.ReflectTaskResults fails while logging the human-readable
	// results; that failure is only logged, the task is still a success.
	require.NoError(t, server.RegisterTask("returns_map", func() (map[string]string, error) {
		return map[string]string{"a": "b"}, nil
	}))

	sig, err := tasks.NewSignature("returns_map", nil)
	require.NoError(t, err)

	require.NoError(t, worker.Process(sig))

	state, err := server.GetBackend().GetState(sig.UUID)
	require.NoError(t, err)
	assert.True(t, state.IsSuccess())
}

func TestWorkerProcessTaskFailedWithErrorHandler(t *testing.T) {
	t.Parallel()

	server, worker := newEagerServerAndWorker(t)
	var handledErr error
	worker.SetErrorHandler(func(err error) { handledErr = err })

	require.NoError(t, server.RegisterTask("failing_task3", func() error {
		return errors.New("kaboom")
	}))

	sig, err := tasks.NewSignature("failing_task3", nil)
	require.NoError(t, err)

	require.NoError(t, worker.Process(sig))
	require.Error(t, handledErr)
	assert.Equal(t, "kaboom", handledErr.Error())
}

func TestWorkerProcessChordCallbackSendTaskError(t *testing.T) {
	t.Parallel()

	publishErr := errors.New("publish failed")
	broker := &fakeBroker{publishErr: publishErr}

	backend := newFakeBackend()
	backend.onGroupCompleted = func(groupUUID string, count int) (bool, error) { return true, nil }
	backend.onTriggerChord = func(groupUUID string) (bool, error) { return true, nil }
	backend.onGroupTaskStates = func(groupUUID string, count int) ([]*tasks.TaskState, error) {
		return []*tasks.TaskState{{TaskUUID: "whatever", State: tasks.StateSuccess}}, nil
	}

	lock, err := machinery.LockFactory(&config.Config{Lock: "eager"})
	require.NoError(t, err)

	server := machinery.NewServerWithBrokerBackendLock(&config.Config{}, broker, backend, lock)
	worker := server.NewWorker("test_worker", 0)

	require.NoError(t, server.RegisterTask("test_task", func() error { return nil }))

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)
	sig.GroupUUID = "some_group"
	sig.GroupTaskCount = 1
	sig.ChordCallback, err = tasks.NewSignature("test_task", nil)
	require.NoError(t, err)

	// worker.Process is called directly (not via server.SendTask), so the
	// main task itself is never published through the faulty broker; only
	// the chord callback's SendTask call at the end of taskSucceeded goes
	// through broker.Publish and fails.
	err = worker.Process(sig)
	assert.ErrorContains(t, err, "publish failed")
}

func TestLaunchCustomQueueLogsCustomQueueName(t *testing.T) {
	cnf := &config.Config{Broker: "eager", DefaultQueue: "machinery_tasks", ResultBackend: "eager", Lock: "eager", NoUnixSignals: true}

	started := make(chan struct{})
	broker := &fakeBroker{
		cnf: cnf,
		startFunc: func(consumerTag string, concurrency int, p brokersiface.TaskProcessor) (bool, error) {
			close(started)
			return false, nil
		},
	}

	backend, err := machinery.BackendFactory(&config.Config{ResultBackend: "eager"})
	require.NoError(t, err)
	lock, err := machinery.LockFactory(&config.Config{Lock: "eager"})
	require.NoError(t, err)

	server := machinery.NewServerWithBrokerBackendLock(cnf, broker, backend, lock)
	worker := server.NewCustomQueueWorker("test_worker", 0, "my_custom_queue")

	errorsChan := make(chan error, 1)
	worker.LaunchAsync(errorsChan)

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for StartConsuming to be called")
	}

	select {
	case err := <-errorsChan:
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker to stop")
	}
}

func TestLaunchRetryWithoutErrorHandlerLogsWarning(t *testing.T) {
	cnf := &config.Config{Broker: "eager", DefaultQueue: "machinery_tasks", ResultBackend: "eager", Lock: "eager", NoUnixSignals: true}

	var calls int32
	broker := &fakeBroker{
		cnf: cnf,
		startFunc: func(consumerTag string, concurrency int, p brokersiface.TaskProcessor) (bool, error) {
			n := atomic.AddInt32(&calls, 1)
			if n == 1 {
				return true, errors.New("connection dropped")
			}
			return false, nil
		},
	}

	backend, err := machinery.BackendFactory(&config.Config{ResultBackend: "eager"})
	require.NoError(t, err)
	lock, err := machinery.LockFactory(&config.Config{Lock: "eager"})
	require.NoError(t, err)

	server := machinery.NewServerWithBrokerBackendLock(cnf, broker, backend, lock)
	worker := server.NewWorker("test_worker", 0)

	errorsChan := make(chan error, 1)
	// No SetErrorHandler call, so the retry branch logs via the default
	// log.WARNING.Printf path instead of invoking a custom handler.
	worker.LaunchAsync(errorsChan)

	select {
	case err := <-errorsChan:
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker to stop retrying")
	}

	assert.GreaterOrEqual(t, atomic.LoadInt32(&calls), int32(2))
}

func TestLaunchAbruptQuitOnSecondSignal(t *testing.T) {
	cnf := &config.Config{Broker: "eager", DefaultQueue: "machinery_tasks", ResultBackend: "eager", Lock: "eager"}

	block := make(chan struct{})
	broker := &fakeBroker{
		cnf: cnf,
		startFunc: func(consumerTag string, concurrency int, p brokersiface.TaskProcessor) (bool, error) {
			<-block
			return false, nil
		},
	}

	backend, err := machinery.BackendFactory(&config.Config{ResultBackend: "eager"})
	require.NoError(t, err)
	lock, err := machinery.LockFactory(&config.Config{Lock: "eager"})
	require.NoError(t, err)

	server := machinery.NewServerWithBrokerBackendLock(cnf, broker, backend, lock)
	worker := server.NewWorker("test_worker", 0)

	errorsChan := make(chan error, 3)
	worker.LaunchAsync(errorsChan)

	require.NoError(t, syscall.Kill(syscall.Getpid(), syscall.SIGTERM))
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, syscall.Kill(syscall.Getpid(), syscall.SIGTERM))

	// Both the graceful goroutine (spawned by the first signal) and the main
	// signal-handling loop (reacting to the second signal) send to this
	// channel without any ordering guarantee between them, so look for
	// ErrWorkerQuitAbruptly across both expected sends rather than assuming
	// it arrives first.
	var sawAbrupt bool
	for i := 0; i < 2; i++ {
		select {
		case err := <-errorsChan:
			if errors.Is(err, machinery.ErrWorkerQuitAbruptly) {
				sawAbrupt = true
			}
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for shutdown errors")
		}
	}
	assert.True(t, sawAbrupt, "expected ErrWorkerQuitAbruptly to be sent after the second signal")

	close(block)
}

func TestLaunchAndQuitGraceful(t *testing.T) {
	cnf := &config.Config{Broker: "eager", DefaultQueue: "machinery_tasks", ResultBackend: "eager", Lock: "eager", NoUnixSignals: true}

	started := make(chan struct{})
	stopped := make(chan struct{})
	var stopOnce int32

	broker := &fakeBroker{
		cnf: cnf,
		startFunc: func(consumerTag string, concurrency int, p brokersiface.TaskProcessor) (bool, error) {
			close(started)
			<-stopped
			return false, nil
		},
		stopFunc: func() {
			if atomic.CompareAndSwapInt32(&stopOnce, 0, 1) {
				close(stopped)
			}
		},
	}

	backend, err := machinery.BackendFactory(&config.Config{ResultBackend: "eager"})
	require.NoError(t, err)
	lock, err := machinery.LockFactory(&config.Config{Lock: "eager"})
	require.NoError(t, err)

	server := machinery.NewServerWithBrokerBackendLock(cnf, broker, backend, lock)
	worker := server.NewWorker("test_worker", 0)

	errorsChan := make(chan error, 2)
	worker.LaunchAsync(errorsChan)

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for StartConsuming to be called")
	}

	worker.Quit()

	select {
	case err := <-errorsChan:
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker to quit")
	}
}

func TestLaunchRetriesOnConsumeError(t *testing.T) {
	cnf := &config.Config{Broker: "eager", DefaultQueue: "machinery_tasks", ResultBackend: "eager", Lock: "eager", NoUnixSignals: true}

	var calls int32
	retryableErr := errors.New("connection dropped")

	broker := &fakeBroker{
		cnf: cnf,
		startFunc: func(consumerTag string, concurrency int, p brokersiface.TaskProcessor) (bool, error) {
			n := atomic.AddInt32(&calls, 1)
			if n == 1 {
				return true, retryableErr
			}
			return false, nil
		},
	}

	backend, err := machinery.BackendFactory(&config.Config{ResultBackend: "eager"})
	require.NoError(t, err)
	lock, err := machinery.LockFactory(&config.Config{Lock: "eager"})
	require.NoError(t, err)

	server := machinery.NewServerWithBrokerBackendLock(cnf, broker, backend, lock)
	worker := server.NewWorker("test_worker", 0)

	var handledErr error
	var handlerCalls int32
	worker.SetErrorHandler(func(err error) {
		handledErr = err
		atomic.AddInt32(&handlerCalls, 1)
	})

	errorsChan := make(chan error, 2)
	worker.LaunchAsync(errorsChan)

	select {
	case err := <-errorsChan:
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker to stop retrying")
	}

	assert.EqualValues(t, 1, atomic.LoadInt32(&handlerCalls))
	assert.ErrorIs(t, handledErr, retryableErr)
	assert.GreaterOrEqual(t, atomic.LoadInt32(&calls), int32(2))
}

func TestLaunchSignalHandlingGraceful(t *testing.T) {
	cnf := &config.Config{Broker: "eager", DefaultQueue: "machinery_tasks", ResultBackend: "eager", Lock: "eager"}

	block := make(chan struct{})
	broker := &fakeBroker{
		cnf: cnf,
		startFunc: func(consumerTag string, concurrency int, p brokersiface.TaskProcessor) (bool, error) {
			<-block
			return false, nil
		},
		stopFunc: func() {
			select {
			case <-block:
			default:
				close(block)
			}
		},
	}

	backend, err := machinery.BackendFactory(&config.Config{ResultBackend: "eager"})
	require.NoError(t, err)
	lock, err := machinery.LockFactory(&config.Config{Lock: "eager"})
	require.NoError(t, err)

	server := machinery.NewServerWithBrokerBackendLock(cnf, broker, backend, lock)
	worker := server.NewWorker("test_worker", 0)

	errorsChan := make(chan error, 2)
	worker.LaunchAsync(errorsChan)

	require.NoError(t, syscall.Kill(syscall.Getpid(), syscall.SIGTERM))

	select {
	case err := <-errorsChan:
		assert.ErrorIs(t, err, machinery.ErrWorkerQuitGracefully)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for graceful shutdown error")
	}
}

func TestLaunchBlocks(t *testing.T) {
	cnf := &config.Config{Broker: "eager", DefaultQueue: "machinery_tasks", ResultBackend: "eager", Lock: "eager", NoUnixSignals: true}

	broker := &fakeBroker{
		cnf: cnf,
		startFunc: func(consumerTag string, concurrency int, p brokersiface.TaskProcessor) (bool, error) {
			return false, nil
		},
	}

	backend, err := machinery.BackendFactory(&config.Config{ResultBackend: "eager"})
	require.NoError(t, err)
	lock, err := machinery.LockFactory(&config.Config{Lock: "eager"})
	require.NoError(t, err)

	server := machinery.NewServerWithBrokerBackendLock(cnf, broker, backend, lock)
	worker := server.NewWorker("test_worker", 0)

	done := make(chan error, 1)
	go func() {
		done <- worker.Launch()
	}()

	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Launch did not return")
	}
}
