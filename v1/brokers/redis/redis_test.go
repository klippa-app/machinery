package redis

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

func TestExecError(t *testing.T) {
	t.Run("all commands succeeded", func(t *testing.T) {
		assert.NoError(t, execError([]interface{}{int64(1), int64(3)}, nil))
	})

	t.Run("transaction aborted", func(t *testing.T) {
		assert.EqualError(t, execError(nil, redis.ErrNil), "transaction aborted")
	})

	t.Run("EXEC itself failed", func(t *testing.T) {
		execAbort := redis.Error("EXECABORT Transaction discarded because of previous errors.")
		assert.Equal(t, execAbort, execError(nil, execAbort))
	})

	t.Run("a command inside the transaction failed", func(t *testing.T) {
		wrongType := redis.Error("WRONGTYPE Operation against a key holding the wrong kind of value")
		err := execError([]interface{}{wrongType, int64(3)}, nil)
		assert.True(t, errors.Is(err, wrongType))
		assert.EqualError(t, err, "command 1 in transaction failed: "+string(wrongType))
	})
}

func TestExecErrorAgainstRedis(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		t.Skip("REDIS_URL is not defined")
	}

	conn, err := redis.Dial("tcp", redisURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	queue := fmt.Sprintf("exec_error_%d", time.Now().UnixNano())
	defer conn.Do("DEL", queue, processingKey(queue, "test"))

	// The processing key holds a string, so LREM fails while RPUSH still runs.
	_, err = conn.Do("SET", processingKey(queue, "test"), "not a list")
	assert.NoError(t, err)

	_ = conn.Send("MULTI")
	_ = conn.Send("LREM", processingKey(queue, "test"), 1, "task")
	_ = conn.Send("RPUSH", queue, "task")
	err = execError(redis.Values(conn.Do("EXEC")))

	assert.ErrorContains(t, err, "command 1 in transaction failed: WRONGTYPE")
	length, err := redis.Int(conn.Do("LLEN", queue))
	assert.NoError(t, err)
	assert.Equal(t, 1, length, "no rollback: RPUSH still ran")
}
