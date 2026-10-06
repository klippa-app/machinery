package redis_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
)

func getRedisURL(t *testing.T) string {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		t.Skip("REDIS_URL is not defined")
	}
	return redisURL
}

func uniqueName(t *testing.T, suffix string) string {
	return fmt.Sprintf("%s_%s_%d", t.Name(), suffix, time.Now().UnixNano())
}

func testConfig(defaultQueue, delayedKey string) *config.Config {
	return &config.Config{
		DefaultQueue: defaultQueue,
		Redis: &config.RedisConfig{
			MaxIdle:                10,
			MaxActive:              100,
			IdleTimeout:            300,
			ReadTimeout:            15,
			WriteTimeout:           15,
			ConnectTimeout:         5,
			NormalTasksPollPeriod:  20,
			DelayedTasksPollPeriod: 20,
			DelayedTasksKey:        delayedKey,
		},
	}
}

func rawRedisClient(redisURL string) goredis.UniversalClient {
	return goredis.NewUniversalClient(&goredis.UniversalOptions{Addrs: []string{redisURL}})
}

func ctxBg() context.Context {
	return context.Background()
}

// testTaskProcessor is a minimal iface.TaskProcessor implementation used to
// drive the broker's consume loop in tests.
type testTaskProcessor struct {
	customQueue string
	preConsume  func() bool
	processFunc func(*tasks.Signature) error
	processed   chan *tasks.Signature
}

func (p *testTaskProcessor) Process(sig *tasks.Signature) error {
	if p.processed != nil {
		p.processed <- sig
	}
	if p.processFunc != nil {
		return p.processFunc(sig)
	}
	return nil
}

func (p *testTaskProcessor) CustomQueue() string {
	return p.customQueue
}

func (p *testTaskProcessor) PreConsumeHandler() bool {
	if p.preConsume != nil {
		return p.preConsume()
	}
	return true
}
