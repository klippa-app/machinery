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
	"github.com/RichardKnop/machinery/v1/brokers/errs"
	redisbroker "github.com/RichardKnop/machinery/v1/brokers/redis"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
)

// reliableQueueEnv runs workers on their own queue, with tasks that record what ran.
type reliableQueueEnv struct {
	t       *testing.T
	queue   string
	cnf     *config.Config
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
			// Short timings, so a dead consumer is found within a test.
			ConsumerHeartbeatPeriod: 100,
			ConsumerTTL:             400,
			ConsumerReapPeriod:      150,
		},
	}
	for _, option := range options {
		option(cnf)
	}

	conn, err := redis.Dial("tcp", redisURL)
	if err != nil {
		t.Fatal(err)
	}

	env := &reliableQueueEnv{
		t:       t,
		queue:   queue,
		cnf:     cnf,
		conn:    conn,
		failed:  map[string]bool{},
		started: make(chan string, 100),
		release: map[string]chan struct{}{},
	}
	env.server = env.newServer()

	t.Cleanup(func() {
		keys, _ := redis.Values(conn.Do("KEYS", queue+"*"))
		if len(keys) > 0 {
			_, _ = conn.Do("DEL", keys...)
		}
		_ = conn.Close()
	})

	return env
}

// newServer returns a server with its own broker, so its own consumer, on the env's queue.
func (e *reliableQueueEnv) newServer() *machinery.Server {
	server, err := machinery.NewServer(e.cnf)
	if err != nil {
		e.t.Fatal(err)
	}

	env := e
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
		e.t.Fatal(err)
	}

	return server
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
	return e.launchOn(e.server, concurrency)
}

func (e *reliableQueueEnv) launchOn(server *machinery.Server, concurrency int) func() {
	worker := server.NewWorker("reliable_queue_worker", concurrency)
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

// processingKey is the processing list of the env's first server.
func (e *reliableQueueEnv) processingKey() string { return e.processingKeyOf(e.server) }

func (e *reliableQueueEnv) processingKeyOf(server *machinery.Server) string {
	return e.queue + ":processing:" + server.GetBroker().(*redisbroker.Broker).ConsumerID()
}

// deadList registers a consumer without a heartbeat, as if its process was killed, and
// returns its processing list.
func (e *reliableQueueEnv) deadList(consumerID string) string {
	if _, err := e.conn.Do("SADD", e.queue+":consumers", consumerID); err != nil {
		e.t.Fatal(err)
	}
	return e.queue + ":processing:" + consumerID
}

func (e *reliableQueueEnv) isConsumer(consumerID string) bool {
	member, err := redis.Bool(e.conn.Do("SISMEMBER", e.queue+":consumers", consumerID))
	if err != nil {
		e.t.Fatal(err)
	}
	return member
}

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

func TestRedisReliableQueue_RecoversDeadConsumerInOrderOnStart(t *testing.T) {
	env := newReliableQueueEnv(t)
	dead := env.deadList("dead")
	env.push(dead, env.signature("record", "left-1"))
	env.push(dead, env.signature("record", "left-2"))
	env.push(env.queue, env.signature("record", "waiting"))

	defer env.launch(1)()

	eventually(t, "all tasks ran", func() bool { return len(env.runs()) == 3 })
	if got := env.runs(); !equalIds(got, "left-1", "left-2", "waiting") {
		t.Errorf("ran %v, want [left-1 left-2 waiting]", got)
	}
	eventually(t, "the lists are empty and the dead consumer is forgotten", func() bool {
		return len(env.ids(env.queue)) == 0 && len(env.ids(env.processingKey())) == 0 &&
			len(env.ids(dead)) == 0 && !env.isConsumer("dead")
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

func (e *reliableQueueEnv) broker() *redisbroker.Broker {
	return e.server.GetBroker().(*redisbroker.Broker)
}

func (e *reliableQueueEnv) recoveries(signature *tasks.Signature) int {
	count, err := redis.Int(e.conn.Do("HGET", e.queue+":recovered", signature.UUID))
	if errors.Is(err, redis.ErrNil) {
		return 0
	}
	if err != nil {
		e.t.Fatal(err)
	}
	return count
}

func TestRedisReliableQueue_CountsRecoveriesAndResetsThemOnAck(t *testing.T) {
	env := newReliableQueueEnv(t)
	left := env.signature("record", "left")
	env.push(env.deadList("dead"), left)
	if _, err := env.conn.Do("HSET", env.queue+":recovered", left.UUID, 1); err != nil {
		t.Fatal(err)
	}

	defer env.launch(1)()

	eventually(t, "the recovered task ran and was acked", func() bool {
		return env.runCount("left") == 1 && len(env.ids(env.processingKey())) == 0
	})
	eventually(t, "its recovery count is reset", func() bool { return env.recoveries(left) == 0 })
	if got := env.ids(env.queue + ":parked"); len(got) != 0 {
		t.Errorf("parked %v, want nothing", got)
	}
}

func TestRedisReliableQueue_ParksTaskOnThirdRecovery(t *testing.T) {
	env := newReliableQueueEnv(t)
	poison := env.signature("record", "poison")
	healthy := env.signature("record", "healthy")
	dead := env.deadList("dead")
	env.push(dead, healthy)
	env.push(dead, poison)
	if _, err := env.conn.Do("HSET", env.queue+":recovered", poison.UUID, 2); err != nil {
		t.Fatal(err)
	}

	type parked struct {
		queue, uuid string
		recoveries  int
	}
	reported := make(chan parked, 1)
	env.broker().SetParkedTaskHandler(func(queue string, signature *tasks.Signature, recoveries int) {
		reported <- parked{queue, signature.UUID, recoveries}
	})

	defer env.launch(1)()

	select {
	case got := <-reported:
		if want := (parked{env.queue, poison.UUID, 3}); got != want {
			t.Errorf("handler got %+v, want %+v", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not called")
	}

	eventually(t, "the healthy task ran", func() bool { return env.runCount("healthy") == 1 })
	if got := env.ids(env.queue + ":parked"); !equalIds(got, "poison") {
		t.Errorf("parked %v, want [poison]", got)
	}
	if env.runCount("poison") != 0 {
		t.Error("the parked task ran")
	}
	if count := env.recoveries(poison); count != 0 {
		t.Errorf("parked task still has recovery count %d", count)
	}
}

func TestRedisReliableQueue_ListsRequeuesAndDeletesParkedTasks(t *testing.T) {
	env := newReliableQueueEnv(t)
	first := env.signature("record", "first")
	second := env.signature("record", "second")
	env.push(env.queue+":parked", first)
	env.push(env.queue+":parked", second)
	broker := env.broker()

	parked, err := broker.GetParkedTasks(env.queue)
	if err != nil {
		t.Fatal(err)
	}
	if len(parked) != 2 || parked[0].UUID != first.UUID || parked[1].UUID != second.UUID {
		t.Fatalf("GetParkedTasks returned %d tasks, want first and second in order", len(parked))
	}

	if err := broker.RequeueParkedTask(env.queue, "unknown"); !errors.Is(err, errs.ErrParkedTaskNotFound) {
		t.Errorf("RequeueParkedTask(unknown) = %v, want ErrParkedTaskNotFound", err)
	}
	if err := broker.DeleteParkedTask(env.queue, "unknown"); !errors.Is(err, errs.ErrParkedTaskNotFound) {
		t.Errorf("DeleteParkedTask(unknown) = %v, want ErrParkedTaskNotFound", err)
	}

	if err := broker.RequeueParkedTask(env.queue, first.UUID); err != nil {
		t.Fatal(err)
	}
	if err := broker.DeleteParkedTask(env.queue, second.UUID); err != nil {
		t.Fatal(err)
	}
	if got := env.ids(env.queue + ":parked"); len(got) != 0 {
		t.Errorf("parked list is %v, want empty", got)
	}

	defer env.launch(1)()
	eventually(t, "the requeued task ran", func() bool { return env.runCount("first") == 1 })
	if env.runCount("second") != 0 {
		t.Error("the deleted task ran")
	}
}

func TestRedisReliableQueue_TwoWorkersNeverShareARunningTask(t *testing.T) {
	env := newReliableQueueEnv(t)
	defer env.launch(1)()
	defer env.releaseAll()

	env.send(env.signature("block", "running"))
	env.waitStarted("running")

	// A second worker starts while the first is busy, as during a deploy.
	second := env.newServer()
	defer env.launchOn(second, 1)()

	// The busy first worker takes up to three of these ahead (one per slot, one buffered, one
	// being handed over); the second runs the rest.
	for _, id := range []string{"new-1", "new-2", "new-3", "new-4"} {
		env.send(env.signature("record", id))
	}
	eventually(t, "the second worker picks up new work", func() bool { return len(env.runs()) >= 2 })

	// Well past the heartbeat TTL, the running task still belongs to the first worker only.
	time.Sleep(time.Second)
	count := func(ids []string) int {
		n := 0
		for _, id := range ids {
			if id == "running" {
				n++
			}
		}
		return n
	}
	if n := count(env.ids(env.processingKey())); n != 1 {
		t.Errorf("first worker's list holds the running task %d times, want 1", n)
	}
	if n := count(env.ids(env.processingKeyOf(second))) + count(env.ids(env.queue)); n != 0 {
		t.Errorf("the running task is %d times in the queue or the second worker's list, want 0", n)
	}

	env.releaseOne("running")
	eventually(t, "every task ran and was acked", func() bool {
		return len(env.runs()) == 5 && len(env.ids(env.processingKey())) == 0 && len(env.ids(env.processingKeyOf(second))) == 0
	})
	if n := env.runCount("running"); n != 1 {
		t.Errorf("running task ran %d times, want 1", n)
	}
}

func TestRedisReliableQueue_RecoversConsumerThatDiesLater(t *testing.T) {
	env := newReliableQueueEnv(t)
	defer env.launch(1)()

	// Registered after startup, so only the periodic check can find it.
	time.Sleep(200 * time.Millisecond)
	env.push(env.deadList("dead"), env.signature("record", "orphan"))

	eventually(t, "the orphaned task ran", func() bool { return env.runCount("orphan") == 1 })
	eventually(t, "the dead consumer is forgotten", func() bool { return !env.isConsumer("dead") })
}

func TestRedisReliableQueue_LeavesLiveConsumersAlone(t *testing.T) {
	env := newReliableQueueEnv(t)
	alive := env.deadList("alive")
	if _, err := env.conn.Do("SET", env.queue+":consumer:alive", 1, "PX", 10000); err != nil {
		t.Fatal(err)
	}
	env.push(alive, env.signature("record", "busy"))

	defer env.launch(1)()

	time.Sleep(time.Second)
	if got := env.ids(alive); !equalIds(got, "busy") {
		t.Errorf("live consumer's list is %v, want [busy]", got)
	}
	if env.runCount("busy") != 0 {
		t.Error("a live consumer's task ran elsewhere")
	}
}

func TestRedisReliableQueue_HeartbeatOutlivesTTLAndStopUnregisters(t *testing.T) {
	env := newReliableQueueEnv(t)
	stop := env.launch(1)
	defer stop()

	id := env.server.GetBroker().(*redisbroker.Broker).ConsumerID()
	heartbeat := env.queue + ":consumer:" + id
	eventually(t, "the worker registers", func() bool { return env.isConsumer(id) })

	time.Sleep(time.Second)
	alive, err := redis.Bool(env.conn.Do("EXISTS", heartbeat))
	if err != nil {
		t.Fatal(err)
	}
	if !alive {
		t.Fatal("heartbeat expired while the worker runs")
	}

	stop()
	alive, err = redis.Bool(env.conn.Do("EXISTS", heartbeat))
	if err != nil {
		t.Fatal(err)
	}
	if alive || env.isConsumer(id) {
		t.Error("a stopped worker is still registered")
	}
}
