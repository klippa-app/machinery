package redis_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/locks/redis"
)

func ctxBg() context.Context {
	return context.Background()
}

func getRedisURL(t *testing.T) string {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		t.Skip("REDIS_URL is not defined")
	}
	return redisURL
}

func uniqueKey(t *testing.T) string {
	return fmt.Sprintf("lock_test_%s_%d", t.Name(), time.Now().UnixNano())
}

func rawClient(redisURL string) goredis.UniversalClient {
	return goredis.NewUniversalClient(&goredis.UniversalOptions{Addrs: []string{redisURL}})
}

func TestNew_NonPositiveRetriesReturnsZeroValue(t *testing.T) {
	assert.NotPanics(t, func() {
		redis.New(new(config.Config), []string{"127.0.0.1:16379"}, 0, 0)
		redis.New(new(config.Config), []string{"127.0.0.1:16379"}, 0, -1)
	})
}

func TestNew_AddressParsingWithoutPassword(t *testing.T) {
	redisURL := getRedisURL(t)

	lock := redis.New(new(config.Config), []string{redisURL}, 0, 1)
	key := uniqueKey(t)

	err := lock.LockWithRetries(context.Background(), key, time.Now().Add(time.Minute).UnixNano())
	assert.NoError(t, err)

	client := rawClient(redisURL)
	defer client.Close()
	defer client.Del(ctxBg(), key)
}

func TestNew_AddressParsingWithPassword(t *testing.T) {
	redisURL := getRedisURL(t)

	// addrs[0] has the form "password@host:port" (no username separator). This
	// exercises the `len(parts) >= 2` branch of address parsing. Since this
	// Redis instance has no password configured, the actual auth will fail,
	// but we only need to exercise the parsing code path here.
	addrs := []string{"somepassword@" + redisURL}
	cnf := &config.Config{Redis: &config.RedisConfig{MasterName: ""}}
	lock := redis.New(cnf, addrs, 0, 1)

	key := uniqueKey(t)
	// Expect this to fail (wrong auth), but it must not panic.
	_ = lock.LockWithRetries(context.Background(), key, time.Now().Add(time.Minute).UnixNano())
}

func TestLock_SuccessOnNewKey(t *testing.T) {
	redisURL := getRedisURL(t)
	client := rawClient(redisURL)
	defer client.Close()

	lock := redis.New(new(config.Config), []string{redisURL}, 0, 1)
	key := uniqueKey(t)
	defer client.Del(ctxBg(), key)

	err := lock.Lock(context.Background(), key, time.Now().Add(time.Minute).UnixNano())
	assert.NoError(t, err)

	v, err := client.Get(ctxBg(), key).Result()
	assert.NoError(t, err)
	assert.NotEmpty(t, v)
}

func TestLock_HeldAndNotExpired(t *testing.T) {
	redisURL := getRedisURL(t)
	client := rawClient(redisURL)
	defer client.Close()

	lock := redis.New(new(config.Config), []string{redisURL}, 0, 1)
	key := uniqueKey(t)
	defer client.Del(ctxBg(), key)

	expire := time.Now().Add(time.Minute).UnixNano()
	assert.NoError(t, lock.Lock(context.Background(), key, expire))

	err := lock.Lock(context.Background(), key, time.Now().Add(2*time.Minute).UnixNano())
	assert.Equal(t, redis.ErrRedisLockFailed, err)
}

func TestLock_GetErrorOnWrongType(t *testing.T) {
	redisURL := getRedisURL(t)
	client := rawClient(redisURL)
	defer client.Close()

	lock := redis.New(new(config.Config), []string{redisURL}, 0, 1)
	key := uniqueKey(t)
	defer client.Del(ctxBg(), key)

	// Create key as a list, so SETNX fails (key exists) but GET on it errors
	// with WRONGTYPE.
	assert.NoError(t, client.RPush(ctxBg(), key, "x").Err())

	err := lock.Lock(context.Background(), key, time.Now().Add(time.Minute).UnixNano())
	assert.Error(t, err)
	assert.NotEqual(t, redis.ErrRedisLockFailed, err)
}

func TestLock_AtoiErrorOnNonNumericValue(t *testing.T) {
	redisURL := getRedisURL(t)
	client := rawClient(redisURL)
	defer client.Close()

	lock := redis.New(new(config.Config), []string{redisURL}, 0, 1)
	key := uniqueKey(t)
	defer client.Del(ctxBg(), key)

	assert.NoError(t, client.Set(ctxBg(), key, "not-a-number", time.Minute).Err())

	err := lock.Lock(context.Background(), key, time.Now().Add(time.Minute).UnixNano())
	assert.Error(t, err)
	assert.NotEqual(t, redis.ErrRedisLockFailed, err)
}

func TestLock_SetNXErrorOnPastExpiry(t *testing.T) {
	redisURL := getRedisURL(t)
	client := rawClient(redisURL)
	defer client.Close()

	lock := redis.New(new(config.Config), []string{redisURL}, 0, 1)
	key := uniqueKey(t)
	defer client.Del(ctxBg(), key)

	// A unixTsToExpireNs far enough in the past makes the derived expiration
	// duration negative, which Redis rejects with "invalid expire time",
	// exercising the SETNX error branch.
	err := lock.Lock(context.Background(), key, time.Now().Add(-time.Hour).UnixNano())
	assert.Error(t, err)
	assert.NotEqual(t, redis.ErrRedisLockFailed, err)
}

func TestLockWithRetries_ExhaustsRetriesAndFails(t *testing.T) {
	redisURL := getRedisURL(t)
	client := rawClient(redisURL)
	defer client.Close()

	lock := redis.New(new(config.Config), []string{redisURL}, 0, 2)
	key := uniqueKey(t)
	defer client.Del(ctxBg(), key)

	assert.NoError(t, lock.Lock(context.Background(), key, time.Now().Add(time.Minute).UnixNano()))

	err := lock.LockWithRetries(context.Background(), key, time.Now().Add(2*time.Minute).UnixNano())
	assert.Equal(t, redis.ErrRedisLockFailed, err)
}

// TestLock_ExpiredContention manufactures an already-"expired" lock value
// (a past timestamp, but with a long physical TTL so Redis itself does not
// evict the key), then races many concurrent Lock calls against it. This
// exercises both the "steal an expired lock" success path (GetSet + Expire)
// and the "someone else already renewed it" ErrRedisLockFailed path, since
// multiple goroutines will race through the Get/GetSet window concurrently.
func TestLock_ExpiredContention(t *testing.T) {
	redisURL := getRedisURL(t)
	client := rawClient(redisURL)
	defer client.Close()

	key := uniqueKey(t)
	defer client.Del(ctxBg(), key)

	pastTS := time.Now().Add(-time.Hour).UnixNano()
	assert.NoError(t, client.Set(ctxBg(), key, pastTS, time.Minute).Err())

	lock := redis.New(new(config.Config), []string{redisURL}, 0, 1)

	const n = 50
	var wg sync.WaitGroup
	results := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- lock.Lock(context.Background(), key, time.Now().Add(time.Hour).UnixNano())
		}()
	}
	wg.Wait()
	close(results)

	var successCount, failCount int
	for err := range results {
		if err == nil {
			successCount++
			continue
		}
		assert.Equal(t, redis.ErrRedisLockFailed, err)
		failCount++
	}

	assert.Equal(t, n, successCount+failCount)
	assert.GreaterOrEqual(t, successCount, 1)
}
