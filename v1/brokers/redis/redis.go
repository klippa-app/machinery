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
	"time"

	"github.com/go-redsync/redsync/v4"
	redsyncgoredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"

	"github.com/RichardKnop/machinery/v1/brokers/errs"
	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
)

const defaultRedisDelayedTasksKey = "delayed_tasks"

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
	deliveries := make(chan []byte, concurrency)
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
					task, _ := b.nextTask(pollCtx, getQueue(b.GetConfig(), taskProcessor))
					//TODO: should this error be ignored?
					if len(task) > 0 {
						deliveries <- task
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

// Publish places a new message on the default queue
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

	return b.client().RPush(ctx, signature.RoutingKey, msg).Err()
}

// GetPendingTasks returns a slice of task signatures waiting in the queue
func (b *Broker) GetPendingTasks(ctx context.Context, queue string) ([]*tasks.Signature, error) {
	if queue == "" {
		queue = b.GetConfig().DefaultQueue
	}
	results, err := b.client().LRange(ctx, queue, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	return decodeSignatures(results)
}

// GetPendingTaskCount returns the number of tasks waiting in the queue
func (b *Broker) GetPendingTaskCount(ctx context.Context, queue string) (int64, error) {
	if queue == "" {
		queue = b.GetConfig().DefaultQueue
	}

	return b.client().LLen(ctx, queue).Result()
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
func (b *Broker) consume(deliveries <-chan []byte, concurrency int, taskProcessor iface.TaskProcessor, stopConsumer chan struct{}) error {
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
			for v := range deliveries {
				b.requeueMessage(v, taskProcessor)
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
					b.requeueMessage(d, taskProcessor)
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
func (b *Broker) consumeOne(delivery []byte, taskProcessor iface.TaskProcessor) error {
	signature := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewReader(delivery))
	decoder.UseNumber()
	if err := decoder.Decode(signature); err != nil {
		return errs.NewErrCouldNotUnmarshalTaskSignature(delivery, err)
	}

	// If the task is not registered, we requeue it,
	// there might be different workers for processing specific tasks
	if !b.IsTaskRegistered(signature.Name) {
		if signature.IgnoreWhenTaskNotRegistered {
			log.ERROR.Printf("Task not registered with this worker. Not requeuing task type %s with UUID %s in queue %s", signature.Name, signature.UUID, signature.RoutingKey)
			return nil
		}
		log.INFO.Printf("Task not registered with this worker. Requeuing task type %s with UUID %s in queue %s", signature.Name, signature.UUID, signature.RoutingKey)
		b.requeueMessage(delivery, taskProcessor)
		return nil
	}

	log.DEBUG.Printf("Received new message: %s", delivery)

	// A fresh context per task, deliberately independent of pollCtx: once a
	// task has been picked up, StopConsuming waits for it to finish rather
	// than cancelling it mid-flight, so it must not be tied to the
	// poll-cancellation context.
	return taskProcessor.Process(context.Background(), signature)
}

// nextTask pops next available task from the default queue
func (b *Broker) nextTask(ctx context.Context, queue string) (result []byte, err error) {
	pollPeriodMilliseconds := 1000 // default poll period for normal tasks
	if b.GetConfig().Redis != nil {
		configuredPollPeriod := b.GetConfig().Redis.NormalTasksPollPeriod
		if configuredPollPeriod > 0 {
			pollPeriodMilliseconds = configuredPollPeriod
		}
	}
	pollPeriod := time.Duration(pollPeriodMilliseconds) * time.Millisecond

	items, err := b.client().BLPop(ctx, pollPeriod, queue).Result()
	if err != nil {
		return []byte{}, err
	}

	// items[0] - the name of the key where an element was popped
	// items[1] - the value of the popped element
	if len(items) != 2 {
		return []byte{}, redis.Nil
	}

	result = []byte(items[1])

	return result, nil
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

func (b *Broker) requeueMessage(delivery []byte, taskProcessor iface.TaskProcessor) {
	// Deliberately context.Background(): this runs during shutdown, and the
	// requeue must still complete so the task isn't lost.
	b.client().RPush(context.Background(), getQueue(b.GetConfig(), taskProcessor), delivery)
}
