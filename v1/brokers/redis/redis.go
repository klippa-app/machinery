package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redsync/redsync/v4"
	redsyncredis "github.com/go-redsync/redsync/v4/redis/redigo"
	"github.com/gomodule/redigo/redis"

	"github.com/RichardKnop/machinery/v1/brokers/errs"
	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
)

const defaultRedisDelayedTasksKey = "delayed_tasks"

// Broker represents a Redis broker.
//
// A picked-up task stays in this consumer's own processing list until it is acked or
// requeued. When a consumer's heartbeat expires, other consumers put its tasks back, so
// tasks survive a killed worker while any number of workers consume the same queue. A task
// recovered maxRecoveries times is parked in "<queue>:parked", as it probably kills the worker.
type Broker struct {
	common.Broker
	common.RedisConnector
	username     string
	host         string
	password     string
	db           int
	pool         *redis.Pool
	consumingWG  sync.WaitGroup // wait group to make sure whole consumption completes
	processingWG sync.WaitGroup // use wait group to make sure task processing completes
	delayedWG    sync.WaitGroup
	// If set, path to a socket file overrides hostname
	socketPath           string
	redsync              *redsync.Redsync
	redisOnce            sync.Once
	redisDelayedTasksKey string
	// consumerID names this consumer's processing list and heartbeat; set by initConsumer.
	consumerID   string
	consumerOnce sync.Once
	// started is set once the consumer is registered; a restart after a consume error keeps it.
	started             atomic.Bool
	consumedQueue       string
	heartbeatPeriod     time.Duration
	consumerTTL         time.Duration
	reapPeriod          time.Duration
	maintenanceStop     chan struct{}
	maintenanceStopOnce sync.Once
	maintenanceWG       sync.WaitGroup
	// parkedTaskHandler is called after recovery parks a task.
	parkedTaskHandler func(queue string, signature *tasks.Signature, recoveries int)
}

// New creates new Broker instance
func New(cnf *config.Config, host, username, password, socketPath string, db int) iface.Broker {
	b := &Broker{Broker: common.NewBroker(cnf)}
	b.host = host
	b.db = db
	b.username = username
	b.password = password
	b.socketPath = socketPath
	b.heartbeatPeriod, b.consumerTTL, b.reapPeriod = consumerTimings(cnf.Redis)

	if cnf.Redis != nil && cnf.Redis.DelayedTasksKey != "" {
		b.redisDelayedTasksKey = cnf.Redis.DelayedTasksKey
	} else {
		b.redisDelayedTasksKey = defaultRedisDelayedTasksKey
	}

	return b
}

// StartConsuming enters a loop and waits for incoming messages
func (b *Broker) StartConsuming(consumerTag string, concurrency int, taskProcessor iface.TaskProcessor) (bool, error) {
	b.consumingWG.Add(1)
	defer b.consumingWG.Done()

	if concurrency < 1 {
		concurrency = runtime.NumCPU() * 2
	}

	b.Broker.StartConsuming(consumerTag, concurrency, taskProcessor)

	// Quit can come before this goroutine runs; opening the pool then races with its Close.
	if !b.GetRetry() {
		return false, nil
	}

	conn := b.open()
	defer conn.Close()

	// Ping the server to make sure connection is live
	_, err := conn.Do("PING")
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

	// Register before picking anything up, so other consumers leave this one's tasks alone.
	if !b.started.Load() {
		if err := b.startConsumer(conn, getQueue(b.GetConfig(), taskProcessor)); err != nil {
			b.GetRetryFunc()(b.GetRetryStopChan())

			if b.GetRetry() {
				return b.GetRetry(), err
			}
			return b.GetRetry(), errs.ErrConsumerStopped
		}

		// Written before started is set, so a reader that sees started also sees the queue.
		b.consumedQueue = getQueue(b.GetConfig(), taskProcessor)
		b.started.Store(true)
	}

	// Channel to which we will push tasks ready for processing by worker
	deliveries := make(chan []byte, concurrency)
	pool := make(chan struct{}, concurrency)
	stopConsumer := make(chan struct{})

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
					task, _ := b.nextTask(getQueue(b.GetConfig(), taskProcessor))
					// TODO: should this error be ignored?
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
	b.delayedWG.Add(1)
	go func() {
		defer b.delayedWG.Done()

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.GetStopChan():
				return
			case <-stopConsumer:
				return
			default:
				task, err := b.nextDelayedTask(b.redisDelayedTasksKey)
				if err != nil {
					continue
				}

				signature := new(tasks.Signature)
				decoder := json.NewDecoder(bytes.NewReader(task))
				decoder.UseNumber()
				if err := decoder.Decode(signature); err != nil {
					log.ERROR.Print(errs.NewErrCouldNotUnmarshalTaskSignature(task, err))
				}

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
	b.processingWG.Wait()

	return b.GetRetry(), nil
}

// StopConsuming quits the loop
func (b *Broker) StopConsuming() {
	b.Broker.StopConsuming()
	// Waiting for the delayed tasks goroutine to have stopped
	b.delayedWG.Wait()
	// Waiting for consumption to finish
	b.consumingWG.Wait()
	// Wait for currently processing tasks to finish as well.
	b.processingWG.Wait()

	b.stopConsumer()

	if b.pool != nil {
		b.pool.Close()
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

	conn := b.open()
	defer conn.Close()

	// Check the ETA signature field, if it is set and it is in the future,
	// delay the task
	if signature.ETA != nil {
		now := time.Now().UTC()

		if signature.ETA.After(now) {
			score := signature.ETA.UnixNano()
			_, err = conn.Do("ZADD", b.redisDelayedTasksKey, score, msg)
			return err
		}
	}

	_, err = conn.Do("RPUSH", signature.RoutingKey, msg)
	return err
}

// GetPendingTasks returns a slice of task signatures waiting in the queue
func (b *Broker) GetPendingTasks(queue string) ([]*tasks.Signature, error) {
	conn := b.open()
	defer conn.Close()

	if queue == "" {
		queue = b.GetConfig().DefaultQueue
	}
	dataBytes, err := conn.Do("LRANGE", queue, 0, -1)
	if err != nil {
		return nil, err
	}
	results, err := redis.ByteSlices(dataBytes, err)
	if err != nil {
		return nil, err
	}

	taskSignatures := make([]*tasks.Signature, len(results))
	for i, result := range results {
		signature := new(tasks.Signature)
		decoder := json.NewDecoder(bytes.NewReader(result))
		decoder.UseNumber()
		if err := decoder.Decode(signature); err != nil {
			return nil, err
		}
		taskSignatures[i] = signature
	}
	return taskSignatures, nil
}

// GetPendingTaskCount returns the number of tasks waiting in the queue
func (b *Broker) GetPendingTaskCount(queue string) (int64, error) {
	conn := b.open()
	defer conn.Close()

	if queue == "" {
		queue = b.GetConfig().DefaultQueue
	}

	dataBytes, err := conn.Do("LLEN", queue)
	if err != nil {
		return 0, err
	}

	count, err := redis.Int64(dataBytes, err)
	if err != nil {
		return 0, err
	}

	return count, nil
}

// GetDelayedTasks returns a slice of task signatures that are scheduled, but not yet in the queue
func (b *Broker) GetDelayedTasks() ([]*tasks.Signature, error) {
	conn := b.open()
	defer conn.Close()

	dataBytes, err := conn.Do("ZRANGE", b.redisDelayedTasksKey, 0, -1)
	if err != nil {
		return nil, err
	}
	results, err := redis.ByteSlices(dataBytes, err)
	if err != nil {
		return nil, err
	}

	taskSignatures := make([]*tasks.Signature, len(results))
	for i, result := range results {
		signature := new(tasks.Signature)
		decoder := json.NewDecoder(bytes.NewReader(result))
		decoder.UseNumber()
		if err := decoder.Decode(signature); err != nil {
			return nil, err
		}
		taskSignatures[i] = signature
	}
	return taskSignatures, nil
}

func (b *Broker) GetDelayedTask(taskUUID string) (*tasks.Signature, error) {
	delayedTasks, err := b.GetDelayedTasks()
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

func (b *Broker) CancelDelayedTask(taskUUID string) error {
	task, err := b.GetDelayedTask(taskUUID)
	if err != nil {
		return err
	}

	taskBytes, err := task.ToJSONByte()
	if err != nil {
		return err
	}

	conn := b.open()
	defer conn.Close()

	_, err = conn.Do("ZREM", b.redisDelayedTasksKey, taskBytes)
	return err
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

			b.processingWG.Add(1)

			// Consume the task inside a goroutine so multiple tasks
			// can be processed concurrently
			go func() {
				// Every delivery leaves the processing list exactly once: requeued or acked.
				taskUUID, requeue, err := b.consumeOne(d, taskProcessor)
				if requeue {
					b.requeueMessage(d, taskProcessor)
				} else {
					b.ackMessage(d, taskUUID, taskProcessor)
				}

				if err != nil {
					errorsChan <- err
				}

				b.processingWG.Done()

				if concurrency > 0 {
					// give slot back to pool
					pool <- struct{}{}
				}
			}()
		}
	}
}

// consumeOne processes a single message using TaskProcessor.
// It returns the task's UUID, empty when unreadable, and whether the message must go back to
// the queue instead of being acked.
func (b *Broker) consumeOne(delivery []byte, taskProcessor iface.TaskProcessor) (string, bool, error) {
	signature := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewReader(delivery))
	decoder.UseNumber()
	if err := decoder.Decode(signature); err != nil {
		return "", false, errs.NewErrCouldNotUnmarshalTaskSignature(delivery, err)
	}

	// If the task is not registered, we requeue it,
	// there might be different workers for processing specific tasks
	if !b.IsTaskRegistered(signature.Name) {
		if signature.IgnoreWhenTaskNotRegistered {
			log.ERROR.Printf("Task not registered with this worker. Not requeuing task type %s with UUID %s in queue %s", signature.Name, signature.UUID, signature.RoutingKey)
			return signature.UUID, false, nil
		}
		log.INFO.Printf("Task not registered with this worker. Requeuing task type %s with UUID %s in queue %s", signature.Name, signature.UUID, signature.RoutingKey)
		return signature.UUID, true, nil
	}

	log.DEBUG.Printf("Received new message: %s", delivery)

	err := taskProcessor.Process(signature)
	return signature.UUID, errors.Is(err, errs.ErrTaskNotStarted), err
}

// nextTask moves the next available task from the queue to its processing list
func (b *Broker) nextTask(queue string) (result []byte, err error) {
	conn := b.open()
	defer conn.Close()

	pollPeriodMilliseconds := 1000 // default poll period for normal tasks
	if b.GetConfig().Redis != nil {
		configuredPollPeriod := b.GetConfig().Redis.NormalTasksPollPeriod
		if configuredPollPeriod > 0 {
			pollPeriodMilliseconds = configuredPollPeriod
		}
	}
	pollPeriod := time.Duration(pollPeriodMilliseconds) * time.Millisecond

	// Issue 548: BLPOP expects an integer timeout expresses in seconds.
	// The call will if the value is a float. Convert to integer using
	// math.Ceil():
	//   math.Ceil(0.0) --> 0 (block indefinitely)
	//   math.Ceil(0.2) --> 1 (timeout after 1 second)
	pollPeriodSeconds := math.Ceil(pollPeriod.Seconds())

	// BLMOVE takes from the head and appends to the processing list in one atomic step, so
	// the task is in Redis at every moment. Returns redis.ErrNil on timeout.
	result, err = redis.Bytes(conn.Do("BLMOVE", queue, b.ownProcessingKey(queue), "LEFT", "RIGHT", pollPeriodSeconds))
	if err != nil {
		return []byte{}, err
	}

	return result, nil
}

// nextDelayedTask pops a value from the ZSET key using WATCH/MULTI/EXEC commands.
// https://github.com/gomodule/redigo/blob/master/redis/zpop_example_test.go
func (b *Broker) nextDelayedTask(key string) (result []byte, err error) {
	conn := b.open()
	defer conn.Close()

	defer func() {
		// Return connection to normal state on error.
		// https://redis.io/commands/discard
		// https://redis.io/commands/unwatch
		if err == redis.ErrNil {
			conn.Do("UNWATCH")
		} else if err != nil {
			conn.Do("DISCARD")
		}
	}()

	var (
		items [][]byte
		reply interface{}
	)

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
		// server with relentless ZRANGEBYSCOREs
		time.Sleep(time.Duration(pollPeriod) * time.Millisecond)
		if _, err = conn.Do("WATCH", key); err != nil {
			return
		}

		now := time.Now().UTC().UnixNano()

		// https://redis.io/commands/zrangebyscore
		items, err = redis.ByteSlices(conn.Do(
			"ZRANGEBYSCORE",
			key,
			0,
			now,
			"LIMIT",
			0,
			1,
		))
		if err != nil {
			return
		}
		if len(items) != 1 {
			err = redis.ErrNil
			return
		}

		_ = conn.Send("MULTI")
		_ = conn.Send("ZREM", key, items[0])
		reply, err = conn.Do("EXEC")
		if err != nil {
			return
		}

		if reply != nil {
			result = items[0]
			break
		}
	}

	return
}

// open returns or creates instance of Redis connection
func (b *Broker) open() redis.Conn {
	b.redisOnce.Do(func() {
		b.pool = b.NewPool(b.socketPath, b.host, b.username, b.password, b.db, b.GetConfig().Redis, b.GetConfig().TLSConfig)
		b.redsync = redsync.New(redsyncredis.NewPool(b.pool))
	})

	return b.pool.Get()
}

func getQueue(config *config.Config, taskProcessor iface.TaskProcessor) string {
	customQueue := taskProcessor.CustomQueue()
	if customQueue == "" {
		return config.DefaultQueue
	}
	return customQueue
}

// requeueMessage moves a picked-up task that did not run back to the tail of the queue.
func (b *Broker) requeueMessage(delivery []byte, taskProcessor iface.TaskProcessor) {
	queue := getQueue(b.GetConfig(), taskProcessor)

	conn := b.open()
	defer conn.Close()

	// One transaction: RPUSH alone would leave a second copy to recover, LREM alone loses it.
	_ = conn.Send("MULTI")
	_ = conn.Send("LREM", b.ownProcessingKey(queue), 1, delivery)
	_ = conn.Send("RPUSH", queue, delivery)
	if err := execError(redis.Values(conn.Do("EXEC"))); err != nil {
		log.ERROR.Printf("Could not requeue task in queue %s: %s", queue, err)
	}
}

// execError returns the error of an EXEC reply: a failed or aborted transaction, or the first
// command that failed inside it. Redis has no rollback, so the other commands still ran.
func execError(replies []interface{}, err error) error {
	if errors.Is(err, redis.ErrNil) {
		return errors.New("transaction aborted")
	}
	if err != nil {
		return err
	}

	for i, reply := range replies {
		if replyErr, ok := reply.(redis.Error); ok {
			return fmt.Errorf("command %d in transaction failed: %w", i+1, replyErr)
		}
	}

	return nil
}

// ackMessage removes a task that ran from the processing list and resets its recovery count.
func (b *Broker) ackMessage(delivery []byte, taskUUID string, taskProcessor iface.TaskProcessor) {
	queue := getQueue(b.GetConfig(), taskProcessor)

	conn := b.open()
	defer conn.Close()

	_ = conn.Send("MULTI")
	_ = conn.Send("LREM", b.ownProcessingKey(queue), 1, delivery)
	_ = conn.Send("HDEL", recoveredKey(queue), taskUUID)
	if err := execError(redis.Values(conn.Do("EXEC"))); err != nil {
		// The task stays in the processing list and runs again on the next start.
		log.ERROR.Printf("Could not ack task in queue %s: %s", queue, err)
	}
}

// SetParkedTaskHandler sets a function that is called each time recovery parks a task.
// Set it before the worker launches.
func (b *Broker) SetParkedTaskHandler(handler func(queue string, signature *tasks.Signature, recoveries int)) {
	b.parkedTaskHandler = handler
}

// GetParkedTasks returns the tasks parked in a queue, oldest first.
func (b *Broker) GetParkedTasks(queue string) ([]*tasks.Signature, error) {
	conn := b.open()
	defer conn.Close()

	items, err := redis.ByteSlices(conn.Do("LRANGE", parkedKey(queue), 0, -1))
	if err != nil {
		return nil, fmt.Errorf("could not read %s: %w", parkedKey(queue), err)
	}

	signatures := make([]*tasks.Signature, 0, len(items))
	for _, item := range items {
		signature, err := decodeSignature(item)
		if err != nil {
			return nil, err
		}
		signatures = append(signatures, signature)
	}
	return signatures, nil
}

// RequeueParkedTask moves a parked task back to the tail of its queue.
// Returns errs.ErrParkedTaskNotFound when no parked task has that UUID.
func (b *Broker) RequeueParkedTask(queue, taskUUID string) error {
	conn := b.open()
	defer conn.Close()

	// WATCH aborts the transaction if the parked list changes after it was read.
	if _, err := conn.Do("WATCH", parkedKey(queue)); err != nil {
		return fmt.Errorf("could not watch %s: %w", parkedKey(queue), err)
	}

	delivery, err := findParked(conn, queue, taskUUID)
	if err != nil {
		return err
	}

	_ = conn.Send("MULTI")
	_ = conn.Send("LREM", parkedKey(queue), 1, delivery)
	_ = conn.Send("RPUSH", queue, delivery)
	if err := execError(redis.Values(conn.Do("EXEC"))); err != nil {
		return fmt.Errorf("could not requeue parked task %s: %w", taskUUID, err)
	}
	return nil
}

// DeleteParkedTask removes a parked task for good.
// Returns errs.ErrParkedTaskNotFound when no parked task has that UUID.
func (b *Broker) DeleteParkedTask(queue, taskUUID string) error {
	conn := b.open()
	defer conn.Close()

	delivery, err := findParked(conn, queue, taskUUID)
	if err != nil {
		return err
	}

	removed, err := redis.Int(conn.Do("LREM", parkedKey(queue), 1, delivery))
	if err != nil {
		return fmt.Errorf("could not delete parked task %s: %w", taskUUID, err)
	}
	if removed == 0 {
		return errs.ErrParkedTaskNotFound
	}
	return nil
}

func findParked(conn redis.Conn, queue, taskUUID string) ([]byte, error) {
	items, err := redis.ByteSlices(conn.Do("LRANGE", parkedKey(queue), 0, -1))
	if err != nil {
		return nil, fmt.Errorf("could not read %s: %w", parkedKey(queue), err)
	}

	for _, item := range items {
		signature, err := decodeSignature(item)
		if err == nil && signature.UUID == taskUUID {
			return item, nil
		}
	}
	return nil, errs.ErrParkedTaskNotFound
}

func decodeSignature(delivery []byte) (*tasks.Signature, error) {
	signature := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewReader(delivery))
	decoder.UseNumber()
	if err := decoder.Decode(signature); err != nil {
		return nil, errs.NewErrCouldNotUnmarshalTaskSignature(delivery, err)
	}
	return signature, nil
}
