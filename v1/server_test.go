package machinery_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"

	eagerbroker "github.com/RichardKnop/machinery/v1/brokers/eager"
)

func TestRegisterTasks(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	err := server.RegisterTasks(map[string]interface{}{
		"test_task": func() error { return nil },
	})
	assert.NoError(t, err)

	_, err = server.GetRegisteredTask("test_task")
	assert.NoError(t, err, "test_task is not registered but it should be")
}

func TestRegisterTask(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	err := server.RegisterTask("test_task", func() error { return nil })
	assert.NoError(t, err)

	_, err = server.GetRegisteredTask("test_task")
	assert.NoError(t, err, "test_task is not registered but it should be")
}

func TestRegisterTasksInvalidTask(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	err := server.RegisterTasks(map[string]interface{}{
		"not_a_func": "this is a string, not a function",
	})
	assert.Error(t, err)
	assert.False(t, server.IsTaskRegistered("not_a_func"))
}

func TestRegisterTaskInvalidTask(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	err := server.RegisterTask("not_a_func", "this is a string, not a function")
	assert.Error(t, err)
	assert.False(t, server.IsTaskRegistered("not_a_func"))
}

func TestRegisterTaskInRaceCondition(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	for i := 0; i < 10; i++ {
		go func() {
			err := server.RegisterTask("test_task", func() error { return nil })
			assert.NoError(t, err)
			_, err = server.GetRegisteredTask("test_task")
			assert.NoError(t, err, "test_task is not registered but it should be")
		}()
	}
}

func TestGetRegisteredTask(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	_, err := server.GetRegisteredTask("test_task")
	assert.Error(t, err, "test_task is registered but it should not be")
}

func TestGetRegisteredTaskNames(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)

	taskName := "test_task"
	err := server.RegisterTask(taskName, func() error { return nil })
	assert.NoError(t, err)

	taskNames := server.GetRegisteredTaskNames()
	assert.Equal(t, 1, len(taskNames))
	assert.Equal(t, taskName, taskNames[0])
}

func TestNewWorker(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)

	server.NewWorker("test_worker", 1)
	assert.NoError(t, nil)
}

func TestNewCustomQueueWorker(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)

	server.NewCustomQueueWorker("test_customqueueworker", 1, "test_queue")
	assert.NoError(t, nil)
}

func getTestServer(t *testing.T) *machinery.Server {
	server, err := machinery.NewServer(&config.Config{
		Broker:        "eager",
		DefaultQueue:  "machinery_tasks",
		ResultBackend: "eager",
		Lock:          "eager",
	})
	if err != nil {
		t.Error(err)
	}
	return server
}

// newEagerServerAndWorker builds a Server around the eager broker/backend/lock
// and returns the single Worker instance that is wired up to actually process
// the tasks it is sent, so the caller can register handlers on it (unlike
// machinery.NewServer, which hides the worker it auto-assigns internally).
func newEagerServerAndWorker(t *testing.T) (*machinery.Server, *machinery.Worker) {
	t.Helper()

	cnf := &config.Config{
		Broker:        "eager",
		DefaultQueue:  "machinery_tasks",
		ResultBackend: "eager",
		Lock:          "eager",
	}

	broker, err := machinery.BrokerFactory(cnf)
	require.NoError(t, err)
	backend, err := machinery.BackendFactory(cnf)
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

func TestServerGetSetBroker(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	original := server.GetBroker()
	assert.NotNil(t, original)

	replacement, err := machinery.BrokerFactory(&config.Config{Broker: "eager"})
	require.NoError(t, err)

	server.SetBroker(replacement)
	assert.Same(t, replacement, server.GetBroker())
}

func TestServerGetSetBackend(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	original := server.GetBackend()
	assert.NotNil(t, original)

	replacement, err := machinery.BackendFactory(&config.Config{ResultBackend: "eager"})
	require.NoError(t, err)

	server.SetBackend(replacement)
	assert.Same(t, replacement, server.GetBackend())
}

func TestServerGetSetConfig(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	assert.Equal(t, "machinery_tasks", server.GetConfig().DefaultQueue)

	newCnf := &config.Config{DefaultQueue: "other_queue"}
	server.SetConfig(newCnf)
	assert.Same(t, newCnf, server.GetConfig())
}

func TestIsTaskRegistered(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	assert.False(t, server.IsTaskRegistered("test_task"))

	require.NoError(t, server.RegisterTask("test_task", func() error { return nil }))
	assert.True(t, server.IsTaskRegistered("test_task"))
}

func TestServerSetPreTaskHandler(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)

	var seen *tasks.Signature
	server.SetPreTaskHandler(func(s *tasks.Signature) {
		seen = s
	})

	require.NoError(t, server.RegisterTask("test_task", func() error { return nil }))

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)

	_, err = server.SendTask(sig)
	require.NoError(t, err)
	require.NotNil(t, seen)
	assert.Equal(t, sig.UUID, seen.UUID)
}

func TestSendTaskSuccess(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	require.NoError(t, server.RegisterTask("add", func(a, b int64) (int64, error) {
		return a + b, nil
	}))

	sig, err := tasks.NewSignature("add", []tasks.Arg{
		{Type: "int64", Value: int64(1)},
		{Type: "int64", Value: int64(2)},
	})
	require.NoError(t, err)

	asyncResult, err := server.SendTask(sig)
	require.NoError(t, err)

	results, err := asyncResult.Get(context.Background(), time.Millisecond)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, int64(3), results[0].Interface())
}

func TestSendTaskWithContext(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	require.NoError(t, server.RegisterTask("test_task", func() error { return nil }))

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)

	asyncResult, err := server.SendTaskWithContext(context.Background(), sig)
	require.NoError(t, err)
	assert.NotNil(t, asyncResult)
}

func TestSendTaskAutoGeneratesUUID(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	require.NoError(t, server.RegisterTask("test_task", func() error { return nil }))

	sig := &tasks.Signature{Name: "test_task"}
	assert.Empty(t, sig.UUID)

	asyncResult, err := server.SendTask(sig)
	require.NoError(t, err)
	assert.NotEmpty(t, sig.UUID)
	assert.Equal(t, sig.UUID, asyncResult.Signature.UUID)
}

func TestSendTaskSetStatePendingError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("pending state failed")
	backend := newFakeBackend()
	backend.onSetStatePending = func(s *tasks.Signature) error { return wantErr }

	broker, err := machinery.BrokerFactory(&config.Config{Broker: "eager"})
	require.NoError(t, err)
	lock, err := machinery.LockFactory(&config.Config{Lock: "eager"})
	require.NoError(t, err)

	server := machinery.NewServerWithBrokerBackendLock(&config.Config{}, broker, backend, lock)

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)

	asyncResult, err := server.SendTask(sig)
	assert.Nil(t, asyncResult)
	assert.ErrorContains(t, err, "pending state failed")
}

func TestSendTaskNoBackend(t *testing.T) {
	t.Parallel()

	broker, err := machinery.BrokerFactory(&config.Config{Broker: "eager"})
	require.NoError(t, err)
	lock, err := machinery.LockFactory(&config.Config{Lock: "eager"})
	require.NoError(t, err)

	server := machinery.NewServerWithBrokerBackendLock(&config.Config{}, broker, nil, lock)

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)

	asyncResult, err := server.SendTask(sig)
	assert.Nil(t, asyncResult)
	assert.EqualError(t, err, "Result backend required")
}

func TestSendTaskUnregistered(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)

	sig, err := tasks.NewSignature("never_registered", nil)
	require.NoError(t, err)

	asyncResult, err := server.SendTask(sig)
	require.NoError(t, err)

	state := asyncResult.GetState(context.Background())
	assert.Equal(t, tasks.StatePending, state.State)
}

func TestSendTaskPublishError(t *testing.T) {
	t.Parallel()

	publishErr := errors.New("publish failed")
	broker := &fakeBroker{publishErr: publishErr}
	backend, err := machinery.BackendFactory(&config.Config{ResultBackend: "eager"})
	require.NoError(t, err)
	lock, err := machinery.LockFactory(&config.Config{Lock: "eager"})
	require.NoError(t, err)

	server := machinery.NewServerWithBrokerBackendLock(&config.Config{}, broker, backend, lock)

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)

	asyncResult, err := server.SendTask(sig)
	assert.Nil(t, asyncResult)
	assert.ErrorContains(t, err, "publish failed")
}

func TestSendChainSuccess(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	require.NoError(t, server.RegisterTask("add", func(a, b int64) (int64, error) {
		return a + b, nil
	}))

	sig1, err := tasks.NewSignature("add", []tasks.Arg{
		{Type: "int64", Value: int64(1)},
		{Type: "int64", Value: int64(2)},
	})
	require.NoError(t, err)
	sig2, err := tasks.NewSignature("add", []tasks.Arg{
		{Type: "int64", Value: int64(10)},
	})
	require.NoError(t, err)

	chain, err := tasks.NewChain(sig1, sig2)
	require.NoError(t, err)

	chainAsyncResult, err := server.SendChain(chain)
	require.NoError(t, err)

	results, err := chainAsyncResult.Get(context.Background(), time.Millisecond)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, int64(13), results[0].Interface())
}

func TestSendChainWithContext(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	require.NoError(t, server.RegisterTask("test_task", func() error { return nil }))

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)
	chain, err := tasks.NewChain(sig)
	require.NoError(t, err)

	chainAsyncResult, err := server.SendChainWithContext(context.Background(), chain)
	require.NoError(t, err)
	assert.NotNil(t, chainAsyncResult)
}

func TestSendChainError(t *testing.T) {
	t.Parallel()

	broker, err := machinery.BrokerFactory(&config.Config{Broker: "eager"})
	require.NoError(t, err)
	lock, err := machinery.LockFactory(&config.Config{Lock: "eager"})
	require.NoError(t, err)

	server := machinery.NewServerWithBrokerBackendLock(&config.Config{}, broker, nil, lock)

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)
	chain, err := tasks.NewChain(sig)
	require.NoError(t, err)

	chainAsyncResult, err := server.SendChain(chain)
	assert.Nil(t, chainAsyncResult)
	assert.EqualError(t, err, "Result backend required")
}

func TestSendGroupSuccess(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	require.NoError(t, server.RegisterTask("add", func(a, b int64) (int64, error) {
		return a + b, nil
	}))

	sig1, err := tasks.NewSignature("add", []tasks.Arg{
		{Type: "int64", Value: int64(1)},
		{Type: "int64", Value: int64(2)},
	})
	require.NoError(t, err)
	sig2, err := tasks.NewSignature("add", []tasks.Arg{
		{Type: "int64", Value: int64(3)},
		{Type: "int64", Value: int64(4)},
	})
	require.NoError(t, err)

	group, err := tasks.NewGroup(sig1, sig2)
	require.NoError(t, err)

	// sendConcurrency 1 (rather than 0, i.e. unlimited) serializes delivery of
	// the group's tasks: the eager backend's state map is only ever touched
	// by one goroutine at a time this way. The eager backend's GetState does
	// not take the same lock its state-writing methods do, so truly
	// concurrent delivery can race on it; see the note in the final report.
	asyncResults, err := server.SendGroup(group, 1)
	require.NoError(t, err)
	require.Len(t, asyncResults, 2)

	results1, err := asyncResults[0].Get(context.Background(), time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, int64(3), results1[0].Interface())

	results2, err := asyncResults[1].Get(context.Background(), time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, int64(7), results2[0].Interface())
}

func TestSendGroupWithConcurrencyLimit(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	require.NoError(t, server.RegisterTask("add", func(a, b int64) (int64, error) {
		return a + b, nil
	}))

	sigs := make([]*tasks.Signature, 0, 5)
	for i := 0; i < 5; i++ {
		sig, err := tasks.NewSignature("add", []tasks.Arg{
			{Type: "int64", Value: int64(i)},
			{Type: "int64", Value: int64(1)},
		})
		require.NoError(t, err)
		sigs = append(sigs, sig)
	}

	group, err := tasks.NewGroup(sigs...)
	require.NoError(t, err)

	// sendConcurrency 1 exercises the same "pool" throttling branch as any
	// other positive value would, while still serializing access to the
	// eager backend's state map (see the comment in TestSendGroupSuccess).
	asyncResults, err := server.SendGroup(group, 1)
	require.NoError(t, err)
	require.Len(t, asyncResults, 5)

	for i, ar := range asyncResults {
		results, err := ar.Get(context.Background(), time.Millisecond)
		require.NoError(t, err)
		assert.Equal(t, int64(i+1), results[0].Interface())
	}
}

func TestSendGroupWithContext(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	require.NoError(t, server.RegisterTask("test_task", func() error { return nil }))

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)
	group, err := tasks.NewGroup(sig)
	require.NoError(t, err)

	asyncResults, err := server.SendGroupWithContext(context.Background(), group, 0)
	require.NoError(t, err)
	assert.Len(t, asyncResults, 1)
}

func TestSendGroupNoBackend(t *testing.T) {
	t.Parallel()

	broker, err := machinery.BrokerFactory(&config.Config{Broker: "eager"})
	require.NoError(t, err)
	lock, err := machinery.LockFactory(&config.Config{Lock: "eager"})
	require.NoError(t, err)

	server := machinery.NewServerWithBrokerBackendLock(&config.Config{}, broker, nil, lock)

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)
	group, err := tasks.NewGroup(sig)
	require.NoError(t, err)

	asyncResults, err := server.SendGroup(group, 0)
	assert.Nil(t, asyncResults)
	assert.EqualError(t, err, "Result backend required")
}

func TestSendGroupPublishError(t *testing.T) {
	t.Parallel()

	publishErr := errors.New("publish failed")
	broker := &fakeBroker{publishErr: publishErr}
	backend, err := machinery.BackendFactory(&config.Config{ResultBackend: "eager"})
	require.NoError(t, err)
	lock, err := machinery.LockFactory(&config.Config{Lock: "eager"})
	require.NoError(t, err)

	server := machinery.NewServerWithBrokerBackendLock(&config.Config{}, broker, backend, lock)

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)
	group, err := tasks.NewGroup(sig)
	require.NoError(t, err)

	asyncResults, err := server.SendGroup(group, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "publish failed")
	assert.Len(t, asyncResults, 1)
}

func TestSendGroupSetStatePendingError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("pending state failed")
	backend := newFakeBackend()
	backend.onSetStatePending = func(s *tasks.Signature) error { return wantErr }

	broker, err := machinery.BrokerFactory(&config.Config{Broker: "eager"})
	require.NoError(t, err)
	lock, err := machinery.LockFactory(&config.Config{Lock: "eager"})
	require.NoError(t, err)

	server := machinery.NewServerWithBrokerBackendLock(&config.Config{}, broker, backend, lock)

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)
	group, err := tasks.NewGroup(sig)
	require.NoError(t, err)

	asyncResults, err := server.SendGroup(group, 0)
	assert.ErrorContains(t, err, "pending state failed")
	assert.Len(t, asyncResults, 1)
}

func TestSendChordSuccess(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	require.NoError(t, server.RegisterTask("add", func(a, b int64) (int64, error) {
		return a + b, nil
	}))
	require.NoError(t, server.RegisterTask("sum", func(nums ...int64) (int64, error) {
		var total int64
		for _, n := range nums {
			total += n
		}
		return total, nil
	}))

	sig1, err := tasks.NewSignature("add", []tasks.Arg{
		{Type: "int64", Value: int64(1)},
		{Type: "int64", Value: int64(2)},
	})
	require.NoError(t, err)
	sig2, err := tasks.NewSignature("add", []tasks.Arg{
		{Type: "int64", Value: int64(3)},
		{Type: "int64", Value: int64(4)},
	})
	require.NoError(t, err)

	group, err := tasks.NewGroup(sig1, sig2)
	require.NoError(t, err)

	callback, err := tasks.NewSignature("sum", nil)
	require.NoError(t, err)

	chord, err := tasks.NewChord(group, callback)
	require.NoError(t, err)

	// sendConcurrency 1, not 0: see the comment in TestSendGroupSuccess about
	// the eager backend's state map not being safe for concurrent access.
	chordAsyncResult, err := server.SendChord(chord, 1)
	require.NoError(t, err)

	results, err := chordAsyncResult.Get(context.Background(), time.Millisecond)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, int64(10), results[0].Interface())
}

func TestSendChordWithContext(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	require.NoError(t, server.RegisterTask("test_task", func() error { return nil }))

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)
	group, err := tasks.NewGroup(sig)
	require.NoError(t, err)
	callback, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)
	chord, err := tasks.NewChord(group, callback)
	require.NoError(t, err)

	chordAsyncResult, err := server.SendChordWithContext(context.Background(), chord, 0)
	require.NoError(t, err)
	assert.NotNil(t, chordAsyncResult)
}

func TestSendChordGroupError(t *testing.T) {
	t.Parallel()

	broker, err := machinery.BrokerFactory(&config.Config{Broker: "eager"})
	require.NoError(t, err)
	lock, err := machinery.LockFactory(&config.Config{Lock: "eager"})
	require.NoError(t, err)

	server := machinery.NewServerWithBrokerBackendLock(&config.Config{}, broker, nil, lock)

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)
	group, err := tasks.NewGroup(sig)
	require.NoError(t, err)
	callback, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)
	chord, err := tasks.NewChord(group, callback)
	require.NoError(t, err)

	chordAsyncResult, err := server.SendChord(chord, 0)
	assert.Nil(t, chordAsyncResult)
	assert.EqualError(t, err, "Result backend required")
}

func TestRegisterPeriodicTask(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	// "@every 1s" fires forever until the scheduler is stopped, so without
	// this the job would keep running (and could even overlap itself under
	// slow, heavily parallel test load) for the rest of the whole test binary.
	t.Cleanup(server.Stop)

	var count int32
	require.NoError(t, server.RegisterTask("periodic_counter", func() error {
		atomic.AddInt32(&count, 1)
		return nil
	}))

	sig, err := tasks.NewSignature("periodic_counter", nil)
	require.NoError(t, err)

	// "@every 1s" is a valid descriptor accepted by cron.ParseStandard and lets
	// the test observe the periodic closure firing without waiting for a real
	// minute boundary like a "* * * * *" spec would require.
	err = server.RegisterPeriodicTask("@every 1s", "test_periodic_task", sig)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&count) >= 1
	}, 3*time.Second, 50*time.Millisecond)
}

func TestRegisterPeriodicTaskInvalidSpec(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)

	err = server.RegisterPeriodicTask("not a valid spec", "test_periodic_task", sig)
	assert.Error(t, err)
}

func TestRegisterPeriodicChain(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	t.Cleanup(server.Stop)

	var count int32
	require.NoError(t, server.RegisterTask("periodic_chain_counter", func() error {
		atomic.AddInt32(&count, 1)
		return nil
	}))

	sig, err := tasks.NewSignature("periodic_chain_counter", nil)
	require.NoError(t, err)

	err = server.RegisterPeriodicChain("@every 1s", "test_periodic_chain", sig)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&count) >= 1
	}, 3*time.Second, 50*time.Millisecond)
}

func TestRegisterPeriodicChainInvalidSpec(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)

	err = server.RegisterPeriodicChain("not a valid spec", "test_periodic_chain", sig)
	assert.Error(t, err)
}

func TestRegisterPeriodicGroup(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	t.Cleanup(server.Stop)

	var count int32
	require.NoError(t, server.RegisterTask("periodic_group_counter", func() error {
		atomic.AddInt32(&count, 1)
		return nil
	}))

	sig, err := tasks.NewSignature("periodic_group_counter", nil)
	require.NoError(t, err)

	err = server.RegisterPeriodicGroup("@every 1s", "test_periodic_group", 0, sig)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&count) >= 1
	}, 3*time.Second, 50*time.Millisecond)
}

func TestRegisterPeriodicGroupInvalidSpec(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)

	err = server.RegisterPeriodicGroup("not a valid spec", "test_periodic_group", 0, sig)
	assert.Error(t, err)
}

func TestRegisterPeriodicChord(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	t.Cleanup(server.Stop)

	var taskCount, callbackCount int32
	require.NoError(t, server.RegisterTask("periodic_chord_task", func() error {
		atomic.AddInt32(&taskCount, 1)
		return nil
	}))
	require.NoError(t, server.RegisterTask("periodic_chord_callback", func() error {
		atomic.AddInt32(&callbackCount, 1)
		return nil
	}))

	sig, err := tasks.NewSignature("periodic_chord_task", nil)
	require.NoError(t, err)
	callback, err := tasks.NewSignature("periodic_chord_callback", nil)
	require.NoError(t, err)

	err = server.RegisterPeriodicChord("@every 1s", "test_periodic_chord", 0, callback, sig)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&callbackCount) >= 1
	}, 3*time.Second, 50*time.Millisecond)
	assert.GreaterOrEqual(t, atomic.LoadInt32(&taskCount), int32(1))
}

func TestRegisterPeriodicChordInvalidSpec(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)
	callback, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)

	err = server.RegisterPeriodicChord("not a valid spec", "test_periodic_chord", 0, callback, sig)
	assert.Error(t, err)
}

// newPeriodicServerWithFailingPublish builds a server around a broker whose
// every Publish call fails, so that the periodic closures registered by
// RegisterPeriodicTask/Chain/Group/Chord reach their "send failed, log it"
// branch instead of their happy path.
func newPeriodicServerWithFailingPublish(t *testing.T) (*machinery.Server, *fakeBroker) {
	t.Helper()

	broker := &fakeBroker{publishErr: errors.New("publish failed")}
	backend, err := machinery.BackendFactory(&config.Config{ResultBackend: "eager"})
	require.NoError(t, err)
	lock, err := machinery.LockFactory(&config.Config{Lock: "eager"})
	require.NoError(t, err)

	return machinery.NewServerWithBrokerBackendLock(&config.Config{}, broker, backend, lock), broker
}

func TestRegisterPeriodicTaskSendFailureIsLogged(t *testing.T) {
	t.Parallel()

	server, broker := newPeriodicServerWithFailingPublish(t)
	t.Cleanup(server.Stop)
	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)

	require.NoError(t, server.RegisterPeriodicTask("@every 1s", "periodic_task_publish_fail", sig))

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&broker.publishCount) >= 1
	}, 3*time.Second, 50*time.Millisecond)
}

func TestRegisterPeriodicChainSendFailureIsLogged(t *testing.T) {
	t.Parallel()

	server, broker := newPeriodicServerWithFailingPublish(t)
	t.Cleanup(server.Stop)
	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)

	require.NoError(t, server.RegisterPeriodicChain("@every 1s", "periodic_chain_publish_fail", sig))

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&broker.publishCount) >= 1
	}, 3*time.Second, 50*time.Millisecond)
}

func TestRegisterPeriodicGroupSendFailureIsLogged(t *testing.T) {
	t.Parallel()

	server, broker := newPeriodicServerWithFailingPublish(t)
	t.Cleanup(server.Stop)
	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)

	require.NoError(t, server.RegisterPeriodicGroup("@every 1s", "periodic_group_publish_fail", 0, sig))

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&broker.publishCount) >= 1
	}, 3*time.Second, 50*time.Millisecond)
}

func TestRegisterPeriodicChordSendFailureIsLogged(t *testing.T) {
	t.Parallel()

	server, broker := newPeriodicServerWithFailingPublish(t)
	t.Cleanup(server.Stop)
	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)
	callback, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)

	require.NoError(t, server.RegisterPeriodicChord("@every 1s", "periodic_chord_publish_fail", 0, callback, sig))

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&broker.publishCount) >= 1
	}, 3*time.Second, 50*time.Millisecond)
}

func TestNewServerEagerAutoAssignsWorker(t *testing.T) {
	t.Parallel()

	server, err := machinery.NewServer(&config.Config{
		Broker:        "eager",
		DefaultQueue:  "machinery_tasks",
		ResultBackend: "eager",
		Lock:          "eager",
	})
	require.NoError(t, err)
	require.NoError(t, server.RegisterTask("test_task", func() error { return nil }))

	sig, err := tasks.NewSignature("test_task", nil)
	require.NoError(t, err)

	asyncResult, err := server.SendTask(sig)
	require.NoError(t, err)

	state := asyncResult.GetState(context.Background())
	assert.Equal(t, tasks.StateSuccess, state.State)
}

func TestNewServerNonEagerBrokerSkipsAutoAssign(t *testing.T) {
	t.Parallel()

	server, err := machinery.NewServer(&config.Config{
		Broker:        "redis://127.0.0.1:1",
		DefaultQueue:  "machinery_tasks",
		ResultBackend: "null",
		Lock:          "eager",
	})
	require.NoError(t, err)
	assert.NotNil(t, server.GetBroker())
	assert.NotNil(t, server.GetBackend())
}

func TestNewServerBrokerFactoryError(t *testing.T) {
	t.Parallel()

	server, err := machinery.NewServer(&config.Config{Broker: "BOGUS"})
	assert.Error(t, err)
	assert.Nil(t, server)
}

func TestNewServerLockFactoryError(t *testing.T) {
	t.Parallel()

	server, err := machinery.NewServer(&config.Config{
		Broker: "eager",
		Lock:   "redis://redis://",
	})
	assert.Error(t, err)
	assert.Nil(t, server)
}
