package integration_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
)

// reliableQueueEnv runs one worker on its own queue, with tasks that record what ran.
type reliableQueueEnv struct {
	t       *testing.T
	queue   string
	server  *machinery.Server
	conn    redis.Conn
	mu      sync.Mutex
	ran     []string
	failed  map[string]bool
	started chan string
	release map[string]chan struct{}
	freed   bool
}

func newReliableQueueEnv(t *testing.T, options ...func(*config.Config)) *reliableQueueEnv {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		t.Skip("REDIS_URL is not defined")
	}

	queue := fmt.Sprintf("reliable_queue_%d", time.Now().UnixNano())
	cnf := &config.Config{
		Broker:        fmt.Sprintf("redis://%v", redisURL),
		DefaultQueue:  queue,
		ResultBackend: fmt.Sprintf("redis://%v", redisURL),
		Lock:          fmt.Sprintf("redis://%v", redisURL),
		// A partial RedisConfig zeroes the pool settings, so spell out common/redis.go's defaults.
		Redis: &config.RedisConfig{
			MaxIdle:                10,
			MaxActive:              100,
			IdleTimeout:            300,
			Wait:                   true,
			ReadTimeout:            15,
			WriteTimeout:           15,
			ConnectTimeout:         15,
			NormalTasksPollPeriod:  100,
			DelayedTasksPollPeriod: 50,
			DelayedTasksKey:        queue + ":delayed",
		},
	}
	for _, option := range options {
		option(cnf)
	}

	server, err := machinery.NewServer(cnf)
	if err != nil {
		t.Fatal(err)
	}

	conn, err := redis.Dial("tcp", redisURL)
	if err != nil {
		t.Fatal(err)
	}

	env := &reliableQueueEnv{
		t:       t,
		queue:   queue,
		server:  server,
		conn:    conn,
		failed:  map[string]bool{},
		started: make(chan string, 100),
		release: map[string]chan struct{}{},
	}

	err = server.RegisterTasks(map[string]interface{}{
		"record": func(id string) error {
			env.record(id)
			return nil
		},
		// flaky fails on its first run only.
		"flaky": func(id string) error {
			env.record(id)
			env.mu.Lock()
			defer env.mu.Unlock()
			if !env.failed[id] {
				env.failed[id] = true
				return errors.New("first run fails")
			}
			return nil
		},
		// block runs until the test releases it.
		"block": func(id string) error {
			env.record(id)
			env.started <- id
			<-env.releaseChan(id)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_, _ = conn.Do("DEL", queue, queue+":processing", queue+":delayed")
		_ = conn.Close()
	})

	return env
}

func (e *reliableQueueEnv) record(id string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ran = append(e.ran, id)
}

func (e *reliableQueueEnv) runs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.ran...)
}

func (e *reliableQueueEnv) runCount(id string) int {
	count := 0
	for _, r := range e.runs() {
		if r == id {
			count++
		}
	}
	return count
}

func (e *reliableQueueEnv) releaseChan(id string) chan struct{} {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.release[id] == nil {
		e.release[id] = make(chan struct{})
		if e.freed {
			close(e.release[id])
		}
	}
	return e.release[id]
}

func (e *reliableQueueEnv) releaseOne(id string) {
	ch := e.releaseChan(id)
	e.mu.Lock()
	defer e.mu.Unlock()
	select {
	case <-ch:
	default:
		close(ch)
	}
}

// releaseAll unblocks every block task, so a failing test can still stop its worker.
func (e *reliableQueueEnv) releaseAll() {
	e.mu.Lock()
	e.freed = true
	ids := make([]string, 0, len(e.release))
	for id := range e.release {
		ids = append(ids, id)
	}
	e.mu.Unlock()

	for _, id := range ids {
		e.releaseOne(id)
	}
}

// launch starts a worker and returns a func that stops it gracefully.
func (e *reliableQueueEnv) launch(concurrency int) func() {
	worker := e.server.NewWorker("reliable_queue_worker", concurrency)
	errorsChan := make(chan error, 1)
	worker.LaunchAsync(errorsChan)

	var once sync.Once
	return func() {
		once.Do(worker.Quit)
	}
}

func (e *reliableQueueEnv) signature(name, id string) *tasks.Signature {
	signature, err := tasks.NewSignature(name, []tasks.Arg{{Type: "string", Value: id}})
	if err != nil {
		e.t.Fatal(err)
	}
	signature.RoutingKey = e.queue
	return signature
}

func (e *reliableQueueEnv) send(signature *tasks.Signature) {
	if _, err := e.server.SendTask(signature); err != nil {
		e.t.Fatal(err)
	}
}

// push puts a task straight into a list, as if a dead process left it there.
func (e *reliableQueueEnv) push(key string, signature *tasks.Signature) {
	raw, err := json.Marshal(signature)
	if err != nil {
		e.t.Fatal(err)
	}
	if _, err := e.conn.Do("RPUSH", key, raw); err != nil {
		e.t.Fatal(err)
	}
}

// ids returns the task ids in a list, in list order.
func (e *reliableQueueEnv) ids(key string) []string {
	items, err := redis.ByteSlices(e.conn.Do("LRANGE", key, 0, -1))
	if err != nil {
		e.t.Fatal(err)
	}

	ids := make([]string, 0, len(items))
	for _, item := range items {
		signature := new(tasks.Signature)
		if err := json.Unmarshal(item, signature); err != nil || len(signature.Args) == 0 {
			ids = append(ids, string(item))
			continue
		}
		ids = append(ids, fmt.Sprint(signature.Args[0].Value))
	}
	return ids
}

func (e *reliableQueueEnv) processingKey() string { return e.queue + ":processing" }

func (e *reliableQueueEnv) waitStarted(id string) {
	select {
	case got := <-e.started:
		if got != id {
			e.t.Fatalf("started %s, want %s", got, id)
		}
	case <-time.After(5 * time.Second):
		e.t.Fatalf("task %s did not start", id)
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func equalIds(got []string, want ...string) bool {
	return fmt.Sprint(got) == fmt.Sprint(want)
}

func TestRedisReliableQueue_AckEmptiesProcessingList(t *testing.T) {
	env := newReliableQueueEnv(t)
	defer env.launch(2)()

	for _, id := range []string{"a", "b", "c"} {
		env.send(env.signature("record", id))
	}

	eventually(t, "all tasks ran and were acked", func() bool {
		return len(env.runs()) == 3 && len(env.ids(env.queue)) == 0 && len(env.ids(env.processingKey())) == 0
	})
}

func TestRedisReliableQueue_RecoversLeftoverTasksInOrderOnStart(t *testing.T) {
	env := newReliableQueueEnv(t)
	env.push(env.processingKey(), env.signature("record", "left-1"))
	env.push(env.processingKey(), env.signature("record", "left-2"))
	env.push(env.queue, env.signature("record", "waiting"))

	defer env.launch(1)()

	eventually(t, "all tasks ran", func() bool { return len(env.runs()) == 3 })
	if got := env.runs(); !equalIds(got, "left-1", "left-2", "waiting") {
		t.Errorf("ran %v, want [left-1 left-2 waiting]", got)
	}
	eventually(t, "both lists are empty", func() bool {
		return len(env.ids(env.queue)) == 0 && len(env.ids(env.processingKey())) == 0
	})
}

func TestRedisReliableQueue_RetryLeavesProcessingListEmpty(t *testing.T) {
	env := newReliableQueueEnv(t)
	defer env.launch(1)()

	signature := env.signature("flaky", "flaky")
	signature.RetryCount = 1
	env.send(signature)

	eventually(t, "the first run failed and was acked", func() bool {
		return env.runCount("flaky") == 1 && len(env.ids(env.processingKey())) == 0
	})

	delayed, err := redis.Int(env.conn.Do("ZCARD", env.queue+":delayed"))
	if err != nil {
		t.Fatal(err)
	}
	if delayed != 1 {
		t.Errorf("%d delayed tasks, want the retry", delayed)
	}

	eventually(t, "the retry ran and was acked", func() bool {
		return env.runCount("flaky") == 2 && len(env.ids(env.processingKey())) == 0
	})
}

func TestRedisReliableQueue_GracefulStopRequeuesPrefetchedTasks(t *testing.T) {
	env := newReliableQueueEnv(t)
	stop := env.launch(1)
	defer stop()
	defer env.releaseAll()

	for _, id := range []string{"running", "prefetched-1", "prefetched-2"} {
		env.send(env.signature("block", id))
	}
	env.waitStarted("running")

	eventually(t, "all three are picked up", func() bool {
		return equalIds(env.ids(env.processingKey()), "running", "prefetched-1", "prefetched-2")
	})

	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()

	// While stopping, the pop loop can pick a requeued task up once more and hand it back
	// again, so only the contents are fixed, not the order.
	eventually(t, "prefetched tasks are back in the queue", func() bool {
		queued := env.ids(env.queue)
		sort.Strings(queued)
		return equalIds(queued, "prefetched-1", "prefetched-2") &&
			equalIds(env.ids(env.processingKey()), "running")
	})

	env.releaseOne("running")
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop")
	}

	if got := env.ids(env.processingKey()); len(got) != 0 {
		t.Errorf("processing list has %v after a graceful stop", got)
	}
	if got := env.runs(); !equalIds(got, "running") {
		t.Errorf("ran %v, want only [running]", got)
	}
}

func TestRedisReliableQueue_ConsumeErrorKeepsRunningTasks(t *testing.T) {
	env := newReliableQueueEnv(t)
	defer env.launch(2)()
	defer env.releaseAll()

	env.send(env.signature("block", "running"))
	env.waitStarted("running")

	// An unreadable message makes the consume loop fail and restart in the same process.
	if _, err := env.conn.Do("RPUSH", env.queue, "not a task"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the unreadable message is acked", func() bool {
		return equalIds(env.ids(env.processingKey()), "running") && len(env.ids(env.queue)) == 0
	})

	// After the restart the worker still consumes, and the running task was not put back.
	env.send(env.signature("record", "after-restart"))
	eventually(t, "a new task runs after the restart", func() bool { return env.runCount("after-restart") == 1 })
	if got := env.ids(env.processingKey()); !equalIds(got, "running") {
		t.Errorf("processing list is %v, want [running]", got)
	}

	env.releaseOne("running")
	eventually(t, "the running task is acked", func() bool { return len(env.ids(env.processingKey())) == 0 })
	if count := env.runCount("running"); count != 1 {
		t.Errorf("running task ran %d times, want 1", count)
	}
}

func TestRedisReliableQueue_RequeueKeepsExactlyOneCopy(t *testing.T) {
	env := newReliableQueueEnv(t)
	defer env.launch(2)()

	// Not registered and not ignored, so the worker keeps handing it back.
	signature := env.signature("unknown", "unknown")
	signature.IgnoreWhenTaskNotRegistered = false
	env.send(signature)

	for i := 0; i < 50; i++ {
		_ = env.conn.Send("MULTI")
		_ = env.conn.Send("LLEN", env.queue)
		_ = env.conn.Send("LLEN", env.processingKey())
		lengths, err := redis.Ints(env.conn.Do("EXEC"))
		if err != nil {
			t.Fatal(err)
		}
		if lengths[0]+lengths[1] != 1 {
			t.Fatalf("queue has %d and processing list %d copies, want 1 in total", lengths[0], lengths[1])
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestRedisReliableQueue_RequeuesTaskThatNeverStarted(t *testing.T) {
	// Nothing listens on port 1, so writing the task state fails before the task runs.
	env := newReliableQueueEnv(t, func(cnf *config.Config) {
		cnf.ResultBackend = "redis://127.0.0.1:1"
	})
	env.push(env.queue, env.signature("record", "never-started"))
	defer env.launch(1)()

	eventually(t, "the worker picked the task up", func() bool {
		return len(env.ids(env.processingKey())) > 0 || len(env.ids(env.queue)) == 0
	})

	for i := 0; i < 25; i++ {
		_ = env.conn.Send("MULTI")
		_ = env.conn.Send("LLEN", env.queue)
		_ = env.conn.Send("LLEN", env.processingKey())
		lengths, err := redis.Ints(env.conn.Do("EXEC"))
		if err != nil {
			t.Fatal(err)
		}
		if lengths[0]+lengths[1] != 1 {
			t.Fatalf("queue has %d and processing list %d copies, want 1 in total", lengths[0], lengths[1])
		}
		time.Sleep(20 * time.Millisecond)
	}

	if got := env.runs(); len(got) != 0 {
		t.Errorf("ran %v, want nothing", got)
	}
}
