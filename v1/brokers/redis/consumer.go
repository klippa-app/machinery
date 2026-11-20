package redis

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/gomodule/redigo/redis"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
)

const (
	defaultConsumerHeartbeatPeriod = 10 * time.Second
	defaultConsumerTTL             = 30 * time.Second
	defaultConsumerReapPeriod      = 15 * time.Second

	// maxRecoveries is the recovery on which a task is parked instead of requeued.
	maxRecoveries = 3
)

// Each consumer keeps its picked-up tasks in "<queue>:processing:<id>" and proves it is alive
// with "<queue>:consumer:<id>", which expires unless refreshed. "<queue>:consumers" lists the
// ids, so a consumer can find the lists of dead consumers and put their tasks back.
func processingKey(queue, consumerID string) string { return queue + ":processing:" + consumerID }
func consumerKey(queue, consumerID string) string   { return queue + ":consumer:" + consumerID }
func consumersKey(queue string) string              { return queue + ":consumers" }
func recoveredKey(queue string) string              { return queue + ":recovered" }
func parkedKey(queue string) string                 { return queue + ":parked" }

// recoverScript moves the last task of a dead consumer's list back to the head of the queue,
// or parks it on its maxRecoveries-th recovery. It returns -1 when the list changed since
// the caller read the task, 0 for a task without UUID, and otherwise the recovery count.
// KEYS: processing list, queue, recovered hash, parked list. ARGV: task, UUID, maxRecoveries.
var recoverScript = redis.NewScript(4, `
if redis.call('LINDEX', KEYS[1], -1) ~= ARGV[1] then
	return -1
end
if ARGV[2] == '' then
	redis.call('LMOVE', KEYS[1], KEYS[2], 'RIGHT', 'LEFT')
	return 0
end
local recoveries = redis.call('HINCRBY', KEYS[3], ARGV[2], 1)
if recoveries >= tonumber(ARGV[3]) then
	redis.call('LMOVE', KEYS[1], KEYS[4], 'RIGHT', 'RIGHT')
	redis.call('HDEL', KEYS[3], ARGV[2])
else
	redis.call('LMOVE', KEYS[1], KEYS[2], 'RIGHT', 'LEFT')
end
return recoveries
`)

func newConsumerID() string {
	random := make([]byte, 8)
	_, _ = rand.Read(random)

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	return hostname + "-" + hex.EncodeToString(random)
}

func consumerTimings(cnf *config.RedisConfig) (heartbeat, ttl, reap time.Duration) {
	heartbeat, ttl, reap = defaultConsumerHeartbeatPeriod, defaultConsumerTTL, defaultConsumerReapPeriod
	if cnf == nil {
		return heartbeat, ttl, reap
	}

	if cnf.ConsumerHeartbeatPeriod > 0 {
		heartbeat = time.Duration(cnf.ConsumerHeartbeatPeriod) * time.Millisecond
	}
	if cnf.ConsumerTTL > 0 {
		ttl = time.Duration(cnf.ConsumerTTL) * time.Millisecond
	}
	if cnf.ConsumerReapPeriod > 0 {
		reap = time.Duration(cnf.ConsumerReapPeriod) * time.Millisecond
	}
	return heartbeat, ttl, reap
}

// initConsumer gives the broker its consumer id on first use rather than in New, so brokers
// built from the same config stay comparable.
func (b *Broker) initConsumer() {
	b.consumerOnce.Do(func() {
		b.consumerID = newConsumerID()
		b.maintenanceStop = make(chan struct{})
	})
}

// ConsumerID returns the id that names this broker's processing list and heartbeat.
func (b *Broker) ConsumerID() string {
	b.initConsumer()
	return b.consumerID
}

func (b *Broker) ownProcessingKey(queue string) string {
	return processingKey(queue, b.ConsumerID())
}

// startConsumer registers this consumer, recovers the tasks of dead consumers, and starts
// the heartbeat and the periodic check for dead consumers.
func (b *Broker) startConsumer(conn redis.Conn, queue string) error {
	b.initConsumer()

	if err := b.register(conn, queue); err != nil {
		return err
	}

	if err := b.reapDeadConsumers(conn, queue); err != nil {
		return err
	}

	b.maintenanceWG.Add(2)
	go b.every(b.heartbeatPeriod, func() error { return b.beat(queue) })
	go b.every(b.reapPeriod, func() error {
		conn := b.open()
		defer conn.Close()

		return b.reapDeadConsumers(conn, queue)
	})

	return nil
}

func (b *Broker) every(period time.Duration, fn func() error) {
	defer b.maintenanceWG.Done()

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-b.maintenanceStop:
			return
		case <-ticker.C:
			if err := fn(); err != nil {
				log.ERROR.Print(err)
			}
		}
	}
}

func (b *Broker) register(conn redis.Conn, queue string) error {
	_ = conn.Send("MULTI")
	_ = conn.Send("SADD", consumersKey(queue), b.consumerID)
	_ = conn.Send("SET", consumerKey(queue, b.consumerID), 1, "PX", b.consumerTTL.Milliseconds())
	if err := execError(redis.Values(conn.Do("EXEC"))); err != nil {
		return fmt.Errorf("could not register consumer %s on %s: %w", b.consumerID, queue, err)
	}
	return nil
}

func (b *Broker) beat(queue string) error {
	conn := b.open()
	defer conn.Close()

	reply, err := conn.Do("SET", consumerKey(queue, b.consumerID), 1, "PX", b.consumerTTL.Milliseconds(), "XX")
	if err != nil {
		return fmt.Errorf("could not refresh the heartbeat of consumer %s on %s: %w", b.consumerID, queue, err)
	}

	if reply == nil {
		// Another consumer may already have put this consumer's running tasks back.
		log.ERROR.Printf("Heartbeat of consumer %s on %s had expired; registering again", b.consumerID, queue)
		return b.register(conn, queue)
	}
	return nil
}

// stopConsumer stops the heartbeat and lets other consumers forget this one. Call it after
// every running task has finished.
func (b *Broker) stopConsumer() {
	b.initConsumer()
	b.maintenanceStopOnce.Do(func() { close(b.maintenanceStop) })
	b.maintenanceWG.Wait()

	if !b.started.Load() {
		return
	}
	queue := b.consumedQueue

	conn := b.open()
	defer conn.Close()

	left, err := redis.Int(conn.Do("LLEN", b.ownProcessingKey(queue)))
	if err != nil {
		log.ERROR.Printf("Could not check the processing list of consumer %s on %s: %s", b.consumerID, queue, err)
		return
	}

	// With tasks left behind, only drop the heartbeat, so another consumer recovers them.
	_ = conn.Send("MULTI")
	_ = conn.Send("DEL", consumerKey(queue, b.consumerID))
	if left == 0 {
		_ = conn.Send("SREM", consumersKey(queue), b.consumerID)
	}
	if err := execError(redis.Values(conn.Do("EXEC"))); err != nil {
		log.ERROR.Printf("Could not unregister consumer %s on %s: %s", b.consumerID, queue, err)
	}
}

// reapDeadConsumers puts back the tasks of every consumer whose heartbeat expired.
func (b *Broker) reapDeadConsumers(conn redis.Conn, queue string) error {
	ids, err := redis.Strings(conn.Do("SMEMBERS", consumersKey(queue)))
	if err != nil {
		return fmt.Errorf("could not list the consumers of %s: %w", queue, err)
	}

	for _, id := range ids {
		if id == b.consumerID {
			continue
		}

		alive, err := redis.Bool(conn.Do("EXISTS", consumerKey(queue, id)))
		if err != nil {
			return fmt.Errorf("could not check consumer %s on %s: %w", id, queue, err)
		}
		if alive {
			continue
		}

		if err := b.recoverConsumer(conn, queue, id); err != nil {
			return err
		}

		// A consumer that comes back registers itself again.
		if _, err := conn.Do("SREM", consumersKey(queue), id); err != nil {
			return fmt.Errorf("could not forget consumer %s on %s: %w", id, queue, err)
		}
	}

	return nil
}

// recoverConsumer moves a dead consumer's tasks back to the head of the queue in their
// original order, or parks those recovered maxRecoveries times.
func (b *Broker) recoverConsumer(conn redis.Conn, queue, consumerID string) error {
	list := processingKey(queue, consumerID)

	for {
		delivery, err := redis.Bytes(conn.Do("LINDEX", list, -1))
		if errors.Is(err, redis.ErrNil) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("could not read %s: %w", list, err)
		}

		// An unreadable task gets an empty UUID: it fails on pickup and is acked there.
		taskUUID, taskName := "", ""
		if signature, err := decodeSignature(delivery); err == nil {
			taskUUID, taskName = signature.UUID, signature.Name
		}

		recoveries, err := redis.Int(recoverScript.Do(conn, list, queue, recoveredKey(queue), parkedKey(queue), delivery, taskUUID, maxRecoveries))
		if err != nil {
			return fmt.Errorf("could not recover a task from %s: %w", list, err)
		}

		switch {
		case recoveries < 0:
			// Another consumer moved it first; read the list again.
		case recoveries == 0:
			log.WARNING.Printf("Recovered unreadable task from %s", list)
		case recoveries < maxRecoveries:
			log.WARNING.Printf("Recovered task type %s with UUID %s from %s (recovery %d of %d)", taskName, taskUUID, list, recoveries, maxRecoveries-1)
		default:
			log.ERROR.Printf("Parked task type %s with UUID %s in %s after %d recoveries", taskName, taskUUID, parkedKey(queue), recoveries)
			if b.parkedTaskHandler != nil {
				signature, _ := decodeSignature(delivery)
				b.parkedTaskHandler(queue, signature, recoveries)
			}
		}
	}
}
