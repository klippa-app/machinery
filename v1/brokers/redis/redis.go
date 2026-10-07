package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redsync/redsync/v4"
	redsyncgoredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/RichardKnop/machinery/v1/brokers/errs"
	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
)

const (
	defaultRedisDelayedTasksKey = "delayed_tasks"

	// partitionListInfix separates a queue name from a partition key in the
	// Redis key used to store that partition's pending tasks, e.g.
	// "my_queue:partition:customer-123".
	partitionListInfix = ":partition:"

	// defaultPartitionLeaseTimeout is used when PartitioningConfig.LeaseTimeout
	// is unset. See PartitioningConfig.LeaseTimeout for what this bounds.
	defaultPartitionLeaseTimeout = time.Hour
)

// acquirePartitionLeaseScript atomically checks a partition's current
// global in-flight count (the number of unexpired lease entries in the
// sorted set at KEYS[1]) against the configured limit, and if there's room,
// records a new lease. This is the source of truth for GlobalConcurrency:
// the caller may use a cheaper, non-atomic pre-check (ZCOUNT) to decide
// which partitions are worth including in a BLPOP call, but must only
// actually treat a task as "acquired" once this script returns 1.
//
// KEYS[1] = partition lease sorted set key
// ARGV[1] = now, as unix nanoseconds (lease scores below this are expired)
// ARGV[2] = GlobalConcurrency limit
// ARGV[3] = this lease's expiry score (now + LeaseTimeout, as unix nanoseconds)
// ARGV[4] = this lease's unique member (the task's UUID)
var acquirePartitionLeaseScript = redis.NewScript(`
local count = redis.call('ZCOUNT', KEYS[1], ARGV[1], '+inf')
if count < tonumber(ARGV[2]) then
	redis.call('ZADD', KEYS[1], ARGV[3], ARGV[4])
	return 1
end
return 0
`)

// Broker represents a Redis broker
type Broker struct {
	common.Broker
	addrs                []string
	db                   int
	socketPath           string
	clientMu             sync.Mutex
	rclient              redis.UniversalClient
	consuming            *waitCounter // tracks in-flight StartConsuming calls
	processing           *waitCounter // tracks in-flight task processing goroutines
	delayed              *waitCounter // tracks the delayed-task watcher goroutine
	redsync              *redsync.Redsync
	redisDelayedTasksKey string
	local                *localPartitionLimiter
	partitionRR          uint64 // rotation cursor for BLPOP key fairness across partitions
}

// New creates new Broker instance. addrs is a list of host:port addresses;
// the first entry may have "user:pass@" or "pass@" prepended to it. When
// socketPath is non-empty, a Unix socket connection is used instead.
func New(cnf *config.Config, addrs []string, db int, socketPath string) iface.Broker {
	b := &Broker{
		Broker:     common.NewBroker(cnf),
		addrs:      addrs,
		db:         db,
		socketPath: socketPath,
		consuming:  newWaitCounter(),
		processing: newWaitCounter(),
		delayed:    newWaitCounter(),
		local:      newLocalPartitionLimiter(),
	}

	if cnf.Redis != nil && cnf.Redis.DelayedTasksKey != "" {
		b.redisDelayedTasksKey = cnf.Redis.DelayedTasksKey
	} else {
		b.redisDelayedTasksKey = defaultRedisDelayedTasksKey
	}
	return b
}

// waitCounter is a race-free alternative to sync.WaitGroup for counters that
// may be incremented (via add) concurrently with a wait call made while the
// count is still zero - an ordering sync.WaitGroup explicitly forbids and
// which the race detector flags, but which legitimately happens here: a
// StopConsuming can race ahead of the first StartConsuming call it is meant
// to wait for.
type waitCounter struct {
	mu    sync.Mutex
	cond  *sync.Cond
	count int
}

func newWaitCounter() *waitCounter {
	w := &waitCounter{}
	w.cond = sync.NewCond(&w.mu)
	return w
}

func (w *waitCounter) add() {
	w.mu.Lock()
	w.count++
	w.mu.Unlock()
}

func (w *waitCounter) done() {
	w.mu.Lock()
	w.count--
	if w.count == 0 {
		w.cond.Broadcast()
	}
	w.mu.Unlock()
}

func (w *waitCounter) wait() {
	w.mu.Lock()
	for w.count > 0 {
		w.cond.Wait()
	}
	w.mu.Unlock()
}

// localPartitionLimiter tracks, in-memory, how many tasks per partition key
// this process currently has in flight, to enforce PartitioningConfig.LocalConcurrency
// without any Redis round trip.
type localPartitionLimiter struct {
	mu     sync.Mutex
	counts map[string]int
}

func newLocalPartitionLimiter() *localPartitionLimiter {
	return &localPartitionLimiter{counts: make(map[string]int)}
}

func (l *localPartitionLimiter) count(key string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.counts[key]
}

func (l *localPartitionLimiter) acquire(key string) {
	l.mu.Lock()
	l.counts[key]++
	l.mu.Unlock()
}

func (l *localPartitionLimiter) release(key string) {
	l.mu.Lock()
	if l.counts[key] > 0 {
		l.counts[key]--
	}
	if l.counts[key] == 0 {
		delete(l.counts, key)
	}
	l.mu.Unlock()
}

// client lazily creates the underlying Redis client, guarded by a mutex so
// construction never races with StopConsuming reading/closing it. Deferring
// construction until first use (rather than doing it eagerly in New) means
// that a StopConsuming racing ahead of the first StartConsuming never has a
// live client to close, so a subsequent lazy connection attempt still
// succeeds instead of hitting an already-closed client.
func (b *Broker) client() redis.UniversalClient {
	b.clientMu.Lock()
	defer b.clientMu.Unlock()
	if b.rclient == nil {
		ropt := common.NewUniversalOptions(b.GetConfig(), b.addrs, b.db, b.socketPath)
		b.rclient = common.NewUniversalClient(b.GetConfig(), ropt)
		b.redsync = redsync.New(redsyncgoredis.NewPool(b.rclient))
	}
	return b.rclient
}

// partitioningConfig returns this broker's partitioning configuration, or
// nil if partitioning isn't enabled (the default).
func (b *Broker) partitioningConfig() *config.PartitioningConfig {
	cnf := b.GetConfig()
	if cnf.Redis == nil {
		return nil
	}
	return cnf.Redis.Partitioning
}

// taskDelivery is a message popped off a queue (or one of its partitions),
// together with enough bookkeeping to release or requeue it correctly.
type taskDelivery struct {
	payload      []byte
	queue        string
	partitionKey string // "" when this task isn't partitioned
	taskUUID     string // only meaningful when partitionKey != ""
}

// StartConsuming enters a loop and waits for incoming messages
func (b *Broker) StartConsuming(consumerTag string, concurrency int, taskProcessor iface.TaskProcessor) (bool, error) {
	b.consuming.add()
	defer b.consuming.done()

	if concurrency < 1 {
		concurrency = runtime.NumCPU() * 2
	}

	b.Broker.StartConsuming(consumerTag, concurrency, taskProcessor)

	// Ping the server to make sure connection is live
	_, err := b.client().Ping(context.Background()).Result()
	if err != nil {
		b.GetRetryFunc()(b.GetRetryStopChan())

		// Return err if retry is still true.
		// If retry is false, broker.StopConsuming() has been called and
		// therefore Redis might have been stopped. Return nil exit
		// StartConsuming()
		if b.GetRetry() {
			return b.GetRetry(), err
		}
		return b.GetRetry(), errs.ErrConsumerStopped
	}

	// Channel to which we will push tasks ready for processing by worker
	deliveries := make(chan taskDelivery, concurrency)
	pool := make(chan struct{}, concurrency)
	stopConsumer := make(chan struct{})

	// pollCtx is cancelled as soon as either stop signal fires, so a
	// blocking BLPOP/WATCH call unblocks immediately instead of waiting out
	// its full poll-period timeout (go-redis enforces a 1 second minimum on
	// blocking command timeouts, so StopConsuming could otherwise stall for
	// up to a second waiting for one of these calls to return on its own).
	pollCtx, cancelPoll := context.WithCancel(context.Background())
	defer cancelPoll()
	go func() {
		select {
		case <-b.GetStopChan():
		case <-stopConsumer:
		}
		cancelPoll()
	}()

	// initialize worker pool with maxWorkers workers
	for i := 0; i < concurrency; i++ {
		pool <- struct{}{}
	}

	// A receiving goroutine keeps popping messages from the queue by BLPOP
	// If the message is valid and can be unmarshaled into a proper structure
	// we send it to the deliveries channel
	go func() {

		log.INFO.Print("[*] Waiting for messages. To exit press CTRL+C")

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.GetStopChan():
				close(deliveries)
				return
			case <-stopConsumer:
				close(deliveries)
				return
			case <-pool:
				select {
				case <-b.GetStopChan():
					close(deliveries)
					return
				default:
				}

				if taskProcessor.PreConsumeHandler() {
					d, err := b.nextTask(pollCtx, getQueue(b.GetConfig(), taskProcessor))
					//TODO: should this error be ignored?
					if err == nil && len(d.payload) > 0 {
						deliveries <- d
					}
				}

				pool <- struct{}{}
			}
		}
	}()

	// A goroutine to watch for delayed tasks and push them to deliveries
	// channel for consumption by the worker
	b.delayed.add()
	go func() {
		defer b.delayed.done()

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.GetStopChan():
				return
			case <-stopConsumer:
				return
			default:
				task, err := b.nextDelayedTask(pollCtx, b.redisDelayedTasksKey)
				if err != nil {
					continue
				}

				signature := new(tasks.Signature)
				decoder := json.NewDecoder(bytes.NewReader(task))
				decoder.UseNumber()
				if err := decoder.Decode(signature); err != nil {
					log.ERROR.Print(errs.NewErrCouldNotUnmarshalTaskSignature(task, err))
				}

				// Deliberately not pollCtx: this republish must still
				// succeed even if it races with shutdown, otherwise the
				// task would be lost rather than moved to the live queue.
				if err := b.Publish(context.Background(), signature); err != nil {
					log.ERROR.Print(err)
				}
			}
		}
	}()

	if err := b.consume(deliveries, concurrency, taskProcessor, stopConsumer); err != nil {
		return b.GetRetry(), err
	}

	// Waiting for any tasks being processed to finish
	b.processing.wait()

	return b.GetRetry(), nil
}

// StopConsuming quits the loop
func (b *Broker) StopConsuming() {
	b.Broker.StopConsuming()
	// Waiting for the delayed tasks goroutine to have stopped
	b.delayed.wait()
	// Waiting for consumption to finish
	b.consuming.wait()
	// Wait for currently processing tasks to finish as well.
	b.processing.wait()

	b.clientMu.Lock()
	client := b.rclient
	b.clientMu.Unlock()
	if client != nil {
		client.Close()
	}
}

// Publish places a new message on the default queue, or on its partition's
// own list when the signature has a PartitionKey and partitioning is
// configured for this broker (see config.PartitioningConfig).
func (b *Broker) Publish(ctx context.Context, signature *tasks.Signature) error {
	// Adjust routing key (this decides which queue the message will be published to)
	b.Broker.AdjustRoutingKey(signature)

	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	// Check the ETA signature field, if it is set and it is in the future,
	// delay the task
	if signature.ETA != nil {
		now := time.Now().UTC()

		if signature.ETA.After(now) {
			score := signature.ETA.UnixNano()
			return b.client().ZAdd(ctx, b.redisDelayedTasksKey, redis.Z{Score: float64(score), Member: msg}).Err()
		}
	}

	if signature.PartitionKey != "" && b.partitioningConfig() != nil {
		listKey := partitionListKey(signature.RoutingKey, signature.PartitionKey)
		pipe := b.client().TxPipeline()
		pipe.RPush(ctx, listKey, msg)
		pipe.SAdd(ctx, partitionsSetKey(signature.RoutingKey), signature.PartitionKey)
		_, err := pipe.Exec(ctx)
		return err
	}

	return b.client().RPush(ctx, signature.RoutingKey, msg).Err()
}

// GetPendingTasks returns a slice of task signatures waiting in the queue,
// including every partition's list when partitioning is configured.
func (b *Broker) GetPendingTasks(ctx context.Context, queue string) ([]*tasks.Signature, error) {
	if queue == "" {
		queue = b.GetConfig().DefaultQueue
	}
	results, err := b.client().LRange(ctx, queue, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	taskSignatures, err := decodeSignatures(results)
	if err != nil {
		return nil, err
	}

	if b.partitioningConfig() == nil {
		return taskSignatures, nil
	}

	partitionKeys, err := b.client().SMembers(ctx, partitionsSetKey(queue)).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	for _, pk := range partitionKeys {
		partResults, err := b.client().LRange(ctx, partitionListKey(queue, pk), 0, -1).Result()
		if err != nil {
			return nil, err
		}
		partSignatures, err := decodeSignatures(partResults)
		if err != nil {
			return nil, err
		}
		taskSignatures = append(taskSignatures, partSignatures...)
	}

	return taskSignatures, nil
}

// GetPendingTaskCount returns the number of tasks waiting in the queue,
// including every partition's list when partitioning is configured.
func (b *Broker) GetPendingTaskCount(ctx context.Context, queue string) (int64, error) {
	if queue == "" {
		queue = b.GetConfig().DefaultQueue
	}

	count, err := b.client().LLen(ctx, queue).Result()
	if err != nil {
		return 0, err
	}

	if b.partitioningConfig() == nil {
		return count, nil
	}

	partitionKeys, err := b.client().SMembers(ctx, partitionsSetKey(queue)).Result()
	if err != nil && err != redis.Nil {
		return 0, err
	}
	for _, pk := range partitionKeys {
		partCount, err := b.client().LLen(ctx, partitionListKey(queue, pk)).Result()
		if err != nil {
			return 0, err
		}
		count += partCount
	}

	return count, nil
}

// GetDelayedTasks returns a slice of task signatures that are scheduled, but not yet in the queue
func (b *Broker) GetDelayedTasks(ctx context.Context) ([]*tasks.Signature, error) {
	results, err := b.client().ZRange(ctx, b.redisDelayedTasksKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	return decodeSignatures(results)
}

// GetDelayedTask returns the delayed task matching the given UUID, if any
func (b *Broker) GetDelayedTask(ctx context.Context, taskUUID string) (*tasks.Signature, error) {
	delayedTasks, err := b.GetDelayedTasks(ctx)
	if err != nil {
		return nil, err
	}

	for _, task := range delayedTasks {
		if task != nil && task.UUID == taskUUID {
			return task, nil
		}
	}

	return nil, errors.New("Not found")
}

// CancelDelayedTask removes a delayed task matching the given UUID from the
// delayed tasks set, preventing it from ever being published
func (b *Broker) CancelDelayedTask(ctx context.Context, taskUUID string) error {
	task, err := b.GetDelayedTask(ctx, taskUUID)
	if err != nil {
		return err
	}

	taskBytes, err := task.ToJSONByte()
	if err != nil {
		return err
	}

	return b.client().ZRem(ctx, b.redisDelayedTasksKey, taskBytes).Err()
}

func decodeSignatures(results []string) ([]*tasks.Signature, error) {
	taskSignatures := make([]*tasks.Signature, len(results))
	for i, result := range results {
		signature := new(tasks.Signature)
		decoder := json.NewDecoder(strings.NewReader(result))
		decoder.UseNumber()
		if err := decoder.Decode(signature); err != nil {
			return nil, err
		}
		taskSignatures[i] = signature
	}
	return taskSignatures, nil
}

// consume takes delivered messages from the channel and manages a worker pool
// to process tasks concurrently
func (b *Broker) consume(deliveries <-chan taskDelivery, concurrency int, taskProcessor iface.TaskProcessor, stopConsumer chan struct{}) error {
	errorsChan := make(chan error, concurrency*2)
	pool := make(chan struct{}, concurrency)

	// init pool for Worker tasks execution, as many slots as Worker concurrency param
	go func() {
		for i := 0; i < concurrency; i++ {
			pool <- struct{}{}
		}
	}()

	for {
		select {
		case err := <-errorsChan:
			close(stopConsumer)
			for d := range deliveries {
				b.requeueDelivery(d, taskProcessor)
			}
			return err
		case d, open := <-deliveries:
			if !open {
				return nil
			}
			if concurrency > 0 {
				// get execution slot from pool (blocks until one is available)
				select {
				case <-b.GetStopChan():
					b.requeueDelivery(d, taskProcessor)
					continue
				case <-pool:
				}
			}

			b.processing.add()

			// Consume the task inside a goroutine so multiple tasks
			// can be processed concurrently
			go func() {
				if err := b.consumeOne(d, taskProcessor); err != nil {
					errorsChan <- err
				}

				b.processing.done()

				if concurrency > 0 {
					// give slot back to pool
					pool <- struct{}{}
				}
			}()
		}
	}
}

// consumeOne processes a single message using TaskProcessor
func (b *Broker) consumeOne(d taskDelivery, taskProcessor iface.TaskProcessor) error {
	signature := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewReader(d.payload))
	decoder.UseNumber()
	if err := decoder.Decode(signature); err != nil {
		b.releasePartitionSlot(d)
		return errs.NewErrCouldNotUnmarshalTaskSignature(d.payload, err)
	}

	// If the task is not registered, we requeue it,
	// there might be different workers for processing specific tasks
	if !b.IsTaskRegistered(signature.Name) {
		if signature.IgnoreWhenTaskNotRegistered {
			log.ERROR.Printf("Task not registered with this worker. Not requeuing task type %s with UUID %s in queue %s", signature.Name, signature.UUID, signature.RoutingKey)
			b.releasePartitionSlot(d)
			return nil
		}
		log.INFO.Printf("Task not registered with this worker. Requeuing task type %s with UUID %s in queue %s", signature.Name, signature.UUID, signature.RoutingKey)
		b.requeueDelivery(d, taskProcessor)
		return nil
	}

	log.DEBUG.Printf("Received new message: %s", d.payload)

	// A fresh context per task, deliberately independent of pollCtx: once a
	// task has been picked up, StopConsuming waits for it to finish rather
	// than cancelling it mid-flight, so it must not be tied to the
	// poll-cancellation context.
	err := taskProcessor.Process(context.Background(), signature)
	b.releasePartitionSlot(d)
	return err
}

// nextTask pops the next available task from the queue, or - when
// partitioning is configured - from whichever of its partitions currently
// has room under both the local and global concurrency limits.
func (b *Broker) nextTask(ctx context.Context, queue string) (taskDelivery, error) {
	if pcnf := b.partitioningConfig(); pcnf != nil {
		return b.nextPartitionedTask(ctx, queue, pcnf)
	}

	payload, err := b.blockingPop(ctx, queue)
	if err != nil {
		return taskDelivery{}, err
	}
	return taskDelivery{payload: payload, queue: queue}, nil
}

// nextPartitionedTask implements the partitioned variant of nextTask. It
// cheaply pre-filters which partitions look eligible (local in-memory check,
// then a non-atomic global ZCOUNT pre-check), BLPOPs across the eligible
// partitions' lists (plus the plain queue, for unpartitioned tasks), and
// then atomically re-checks and records the global lease for whichever
// partition it actually popped from. If that atomic check loses a race
// against another worker, the task is pushed back to the front of its
// partition's list and the loop tries again.
func (b *Broker) nextPartitionedTask(ctx context.Context, queue string, pcnf *config.PartitioningConfig) (taskDelivery, error) {
	for {
		partitionKeys, err := b.client().SMembers(ctx, partitionsSetKey(queue)).Result()
		if err != nil && err != redis.Nil {
			return taskDelivery{}, err
		}

		eligible := make([]string, 0, len(partitionKeys))
		for _, pk := range partitionKeys {
			if pcnf.LocalConcurrency > 0 && b.local.count(pk) >= pcnf.LocalConcurrency {
				continue
			}
			eligible = append(eligible, pk)
		}

		if pcnf.GlobalConcurrency > 0 && len(eligible) > 0 {
			eligible = b.filterGloballyEligible(ctx, queue, eligible, pcnf.GlobalConcurrency)
		}

		candidates := make([]string, 0, len(eligible)+1)
		for _, pk := range eligible {
			candidates = append(candidates, partitionListKey(queue, pk))
		}
		// Always include the plain queue, both as a fallback so BLPOP always
		// has at least one key to watch, and so unpartitioned tasks (those
		// published without a PartitionKey) keep flowing normally even when
		// partitioning is configured.
		candidates = append(candidates, queue)
		candidates = rotate(candidates, int(atomic.AddUint64(&b.partitionRR, 1)))

		payload, poppedKey, err := b.blockingPopFrom(ctx, candidates...)
		if err != nil {
			return taskDelivery{}, err
		}

		if poppedKey == queue {
			return taskDelivery{payload: payload, queue: queue}, nil
		}

		partitionKey := strings.TrimPrefix(poppedKey, queue+partitionListInfix)
		d := taskDelivery{payload: payload, queue: queue, partitionKey: partitionKey, taskUUID: decodeTaskUUID(payload)}

		if pcnf.GlobalConcurrency <= 0 {
			b.local.acquire(partitionKey)
			return d, nil
		}

		leaseTimeout := pcnf.LeaseTimeout
		if leaseTimeout <= 0 {
			leaseTimeout = defaultPartitionLeaseTimeout
		}
		now := time.Now()
		member := d.taskUUID
		if member == "" {
			// Defensive: every signature should have a UUID, but don't let a
			// missing one silently defeat the global limit.
			member = uuid.New().String()
		}

		acquired, luaErr := acquirePartitionLeaseScript.Run(
			ctx, b.client(), []string{partitionLeaseKey(queue, partitionKey)},
			now.UnixNano(), pcnf.GlobalConcurrency, now.Add(leaseTimeout).UnixNano(), member,
		).Int()
		if luaErr != nil || acquired == 0 {
			// Either the script failed, or we lost the race against another
			// worker for this partition's last slot between our pre-check
			// and this atomic acquire. Put the task back at the front of its
			// own partition list to preserve order, and try again.
			b.client().LPush(context.Background(), partitionListKey(queue, partitionKey), payload)
			continue
		}

		b.local.acquire(partitionKey)
		return d, nil
	}
}

// filterGloballyEligible returns the subset of partitionKeys whose current
// global in-flight lease count is below limit. This is a non-atomic,
// best-effort pre-check used only to decide which partitions are worth
// including in the BLPOP call; the actual limit is enforced atomically by
// acquirePartitionLeaseScript after a task has been popped.
func (b *Broker) filterGloballyEligible(ctx context.Context, queue string, partitionKeys []string, limit int) []string {
	client := b.client()
	pipe := client.Pipeline()
	cmds := make([]*redis.IntCmd, len(partitionKeys))
	now := strconv.FormatInt(time.Now().UnixNano(), 10)
	for i, pk := range partitionKeys {
		cmds[i] = pipe.ZCount(ctx, partitionLeaseKey(queue, pk), now, "+inf")
	}
	_, _ = pipe.Exec(ctx)

	eligible := make([]string, 0, len(partitionKeys))
	for i, pk := range partitionKeys {
		count, err := cmds[i].Result()
		if err != nil {
			// Be optimistic on pipeline errors: the atomic acquire step
			// afterwards is the real guard against exceeding the limit.
			count = 0
		}
		if count < int64(limit) {
			eligible = append(eligible, pk)
		}
	}
	return eligible
}

// releasePartitionSlot releases the local (and, if configured, global)
// concurrency slot held by a partitioned delivery. It is a no-op for
// unpartitioned deliveries.
func (b *Broker) releasePartitionSlot(d taskDelivery) {
	if d.partitionKey == "" {
		return
	}
	b.local.release(d.partitionKey)

	pcnf := b.partitioningConfig()
	if pcnf == nil || pcnf.GlobalConcurrency <= 0 {
		return
	}

	// Deliberately context.Background(): this must complete regardless of
	// shutdown timing, otherwise the lease lingers until LeaseTimeout.
	ctx := context.Background()
	client := b.client()
	leaseKey := partitionLeaseKey(d.queue, d.partitionKey)
	if d.taskUUID != "" {
		client.ZRem(ctx, leaseKey, d.taskUUID)
	}

	// Opportunistic cleanup so the partitions registry doesn't grow
	// unbounded with long-dead keys. Harmless if this races with a
	// concurrent publish to the same partition key: the SAdd on publish
	// would simply re-add it.
	pipe := client.Pipeline()
	llenCmd := pipe.LLen(ctx, partitionListKey(d.queue, d.partitionKey))
	zcardCmd := pipe.ZCard(ctx, leaseKey)
	_, _ = pipe.Exec(ctx)
	if llenCmd.Val() == 0 && zcardCmd.Val() == 0 {
		client.SRem(ctx, partitionsSetKey(d.queue), d.partitionKey)
	}
}

// requeueDelivery puts a delivery back on the queue it came from (its own
// partition's list, if partitioned) and releases any concurrency slot it
// was holding, since this worker is no longer processing it.
func (b *Broker) requeueDelivery(d taskDelivery, taskProcessor iface.TaskProcessor) {
	target := getQueue(b.GetConfig(), taskProcessor)
	if d.partitionKey != "" {
		target = partitionListKey(target, d.partitionKey)
	}
	// Deliberately context.Background(): this runs during shutdown/error
	// handling, and the requeue must still complete so the task isn't lost.
	b.client().RPush(context.Background(), target, d.payload)
	b.releasePartitionSlot(d)
}

func partitionsSetKey(queue string) string {
	return queue + ":partitions"
}

func partitionListKey(queue, partitionKey string) string {
	return queue + partitionListInfix + partitionKey
}

func partitionLeaseKey(queue, partitionKey string) string {
	return partitionListKey(queue, partitionKey) + ":leases"
}

// decodeTaskUUID extracts just the UUID field from a marshaled signature,
// without paying the cost of decoding the whole thing - the full signature
// is decoded again (and used) in consumeOne once it's actually dispatched.
func decodeTaskUUID(payload []byte) string {
	var head struct {
		UUID string
	}
	_ = json.Unmarshal(payload, &head)
	return head.UUID
}

// rotate returns a copy of s rotated left by by positions, so repeated
// calls with an incrementing by spread the "first" position evenly across
// all of s - used so BLPOP's "first non-empty key wins" tie-break doesn't
// always favor the same partition.
func rotate(s []string, by int) []string {
	n := len(s)
	if n <= 1 {
		return s
	}
	by = by % n
	rotated := make([]string, 0, n)
	rotated = append(rotated, s[by:]...)
	rotated = append(rotated, s[:by]...)
	return rotated
}

// blockingPop pops the next value from a single list key.
func (b *Broker) blockingPop(ctx context.Context, queue string) ([]byte, error) {
	payload, _, err := b.blockingPopFrom(ctx, queue)
	return payload, err
}

// blockingPopFrom pops the next value from whichever of keys is non-empty
// first (per Redis BLPOP semantics: keys are checked in the given order),
// blocking until the configured normal-task poll period elapses.
func (b *Broker) blockingPopFrom(ctx context.Context, keys ...string) (payload []byte, poppedKey string, err error) {
	pollPeriodMilliseconds := 1000 // default poll period for normal tasks
	if b.GetConfig().Redis != nil {
		configuredPollPeriod := b.GetConfig().Redis.NormalTasksPollPeriod
		if configuredPollPeriod > 0 {
			pollPeriodMilliseconds = configuredPollPeriod
		}
	}
	pollPeriod := time.Duration(pollPeriodMilliseconds) * time.Millisecond

	items, err := b.client().BLPop(ctx, pollPeriod, keys...).Result()
	if err != nil {
		return nil, "", err
	}

	// items[0] - the name of the key where an element was popped
	// items[1] - the value of the popped element
	if len(items) != 2 {
		return nil, "", redis.Nil
	}

	return []byte(items[1]), items[0], nil
}

// nextDelayedTask pops a value from the ZSET key using WATCH/MULTI/EXEC commands.
func (b *Broker) nextDelayedTask(ctx context.Context, key string) (result []byte, err error) {
	var items []string

	pollPeriod := 500 // default poll period for delayed tasks
	if b.GetConfig().Redis != nil {
		configuredPollPeriod := b.GetConfig().Redis.DelayedTasksPollPeriod
		// the default period is 0, which bombards redis with requests, despite
		// our intention of doing the opposite
		if configuredPollPeriod > 0 {
			pollPeriod = configuredPollPeriod
		}
	}

	for {
		// Space out queries to ZSET so we don't bombard redis
		// server with relentless ZRANGEBYSCOREs, but stay responsive to stop
		// so StopConsuming doesn't have to wait out a full poll period.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(pollPeriod) * time.Millisecond):
		}
		watchFunc := func(tx *redis.Tx) error {

			now := time.Now().UTC().UnixNano()

			// https://redis.io/commands/zrangebyscore
			items, err = tx.ZRevRangeByScore(ctx, key, &redis.ZRangeBy{
				Min: "0", Max: strconv.FormatInt(now, 10), Offset: 0, Count: 1,
			}).Result()
			if err != nil {
				return err
			}
			if len(items) != 1 {
				return redis.Nil
			}

			// only return the first zrange value if there are no other changes in this key
			// to make sure a delayed task would only be consumed once
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.ZRem(ctx, key, items[0])
				result = []byte(items[0])
				return nil
			})

			return err
		}

		if err = b.client().Watch(ctx, watchFunc, key); err != nil {
			return
		} else {
			break
		}
	}

	return
}

func getQueue(config *config.Config, taskProcessor iface.TaskProcessor) string {
	customQueue := taskProcessor.CustomQueue()
	if customQueue == "" {
		return config.DefaultQueue
	}
	return customQueue
}
