package redis_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/go-redsync/redsync/v4"
	redsyncgoredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	goredis "github.com/redis/go-redis/v9"

	"github.com/RichardKnop/machinery/v1/backends/iface"
	"github.com/RichardKnop/machinery/v1/backends/redis"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getRedisURLString() string {
	return os.Getenv("REDIS_URL")
}

func getRedis() iface.Backend {
	// host1:port1,host2:port2
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		return nil
	}
	backend := redis.New(new(config.Config), strings.Split(redisURL, ","), 0, "")
	return backend
}

func TestGroupCompleted(t *testing.T) {
	backend := getRedis()
	if backend == nil {
		t.Skip("REDIS_URL is not defined")
	}

	groupUUID := "testGroupUUID"
	task1 := &tasks.Signature{
		UUID:      "testTaskUUID1",
		GroupUUID: groupUUID,
	}
	task2 := &tasks.Signature{
		UUID:      "testTaskUUID2",
		GroupUUID: groupUUID,
	}

	// Cleanup before the test
	backend.PurgeState(context.Background(), task1.UUID)
	backend.PurgeState(context.Background(), task2.UUID)
	backend.PurgeGroupMeta(context.Background(), groupUUID)

	groupCompleted, err := backend.GroupCompleted(context.Background(), groupUUID, 2)
	if assert.Error(t, err) {
		assert.False(t, groupCompleted)
		assert.Equal(t, "redis: nil", err.Error())
	}

	backend.InitGroup(context.Background(), groupUUID, []string{task1.UUID, task2.UUID})

	groupCompleted, err = backend.GroupCompleted(context.Background(), groupUUID, 2)
	if assert.Error(t, err) {
		assert.False(t, groupCompleted)
		assert.Equal(t, "redis: nil", err.Error())
	}

	backend.SetStatePending(context.Background(), task1)
	backend.SetStateStarted(context.Background(), task2)
	groupCompleted, err = backend.GroupCompleted(context.Background(), groupUUID, 2)
	if assert.NoError(t, err) {
		assert.False(t, groupCompleted)
	}

	taskResults := []*tasks.TaskResult{new(tasks.TaskResult)}
	backend.SetStateStarted(context.Background(), task1)
	backend.SetStateSuccess(context.Background(), task2, taskResults)
	groupCompleted, err = backend.GroupCompleted(context.Background(), groupUUID, 2)
	if assert.NoError(t, err) {
		assert.False(t, groupCompleted)
	}

	backend.SetStateFailure(context.Background(), task1, "Some error")
	groupCompleted, err = backend.GroupCompleted(context.Background(), groupUUID, 2)
	if assert.NoError(t, err) {
		assert.True(t, groupCompleted)
	}
}

func TestGetState(t *testing.T) {
	backend := getRedis()
	if backend == nil {
		t.Skip("REDIS_URL is not defined")
	}

	signature := &tasks.Signature{
		UUID:      "testTaskUUID",
		GroupUUID: "testGroupUUID",
	}

	backend.PurgeState(context.Background(), "testTaskUUID")

	var (
		taskState *tasks.TaskState
		err       error
	)

	taskState, err = backend.GetState(context.Background(), signature.UUID)
	assert.Equal(t, "redis: nil", err.Error())
	assert.Nil(t, taskState)

	//Pending State
	backend.SetStatePending(context.Background(), signature)
	taskState, err = backend.GetState(context.Background(), signature.UUID)
	assert.NoError(t, err)
	assert.Equal(t, signature.Name, taskState.TaskName)
	createdAt := taskState.CreatedAt

	//Received State
	backend.SetStateReceived(context.Background(), signature)
	taskState, err = backend.GetState(context.Background(), signature.UUID)
	assert.NoError(t, err)
	assert.Equal(t, signature.Name, taskState.TaskName)
	assert.Equal(t, createdAt, taskState.CreatedAt)

	//Started State
	backend.SetStateStarted(context.Background(), signature)
	taskState, err = backend.GetState(context.Background(), signature.UUID)
	assert.NoError(t, err)
	assert.Equal(t, signature.Name, taskState.TaskName)
	assert.Equal(t, createdAt, taskState.CreatedAt)

	//Success State
	taskResults := []*tasks.TaskResult{
		{
			Type:  "float64",
			Value: 2,
		},
	}
	backend.SetStateSuccess(context.Background(), signature, taskResults)
	taskState, err = backend.GetState(context.Background(), signature.UUID)
	assert.NoError(t, err)
	assert.Equal(t, signature.Name, taskState.TaskName)
	assert.Equal(t, createdAt, taskState.CreatedAt)
	assert.NotNil(t, taskState.Results)
}

func TestPurgeState(t *testing.T) {
	backend := getRedis()
	if backend == nil {
		t.Skip("REDIS_URL is not defined")
	}

	signature := &tasks.Signature{
		UUID:      "testTaskUUID",
		GroupUUID: "testGroupUUID",
	}

	backend.SetStatePending(context.Background(), signature)
	taskState, err := backend.GetState(context.Background(), signature.UUID)
	assert.NotNil(t, taskState)
	assert.NoError(t, err)

	backend.PurgeState(context.Background(), taskState.TaskUUID)
	taskState, err = backend.GetState(context.Background(), signature.UUID)
	assert.Nil(t, taskState)
	assert.Error(t, err)
}

func TestGroupTaskStates(t *testing.T) {
	backend := getRedis()
	if backend == nil {
		t.Skip("REDIS_URL is not defined")
	}

	groupUUID := "testGroupUUIDStates"
	task1 := &tasks.Signature{UUID: "testTaskUUIDStates1", GroupUUID: groupUUID}
	task2 := &tasks.Signature{UUID: "testTaskUUIDStates2", GroupUUID: groupUUID}

	backend.PurgeState(context.Background(), task1.UUID)
	backend.PurgeState(context.Background(), task2.UUID)
	backend.PurgeGroupMeta(context.Background(), groupUUID)

	_, err := backend.GroupTaskStates(context.Background(), groupUUID, 2)
	assert.Error(t, err)

	backend.InitGroup(context.Background(), groupUUID, []string{task1.UUID, task2.UUID})
	backend.SetStatePending(context.Background(), task1)
	backend.SetStateSuccess(context.Background(), task2, []*tasks.TaskResult{new(tasks.TaskResult)})

	states, err := backend.GroupTaskStates(context.Background(), groupUUID, 2)
	assert.NoError(t, err)
	assert.Len(t, states, 2)
}

func TestTriggerChord(t *testing.T) {
	backend := getRedis()
	if backend == nil {
		t.Skip("REDIS_URL is not defined")
	}

	groupUUID := "testGroupUUIDChord"
	backend.PurgeGroupMeta(context.Background(), groupUUID)

	_, err := backend.TriggerChord(context.Background(), groupUUID)
	assert.Error(t, err)

	backend.InitGroup(context.Background(), groupUUID, []string{"t1"})

	triggered, err := backend.TriggerChord(context.Background(), groupUUID)
	assert.NoError(t, err)
	assert.True(t, triggered)

	triggered, err = backend.TriggerChord(context.Background(), groupUUID)
	assert.NoError(t, err)
	assert.False(t, triggered)
}

// TestTriggerChordLockContention holds the "TriggerChordMutex" distributed
// lock externally (via our own redsync instance against the same Redis), so
// that TriggerChord's own m.Lock() call fails to acquire it within its retry
// budget, exercising that error branch.
func TestTriggerChordLockContention(t *testing.T) {
	redisURL := getRedisURLString()
	if redisURL == "" {
		t.Skip("REDIS_URL is not defined")
	}

	groupUUID := "testGroupUUIDChordLock"
	backend := getRedis()
	defer backend.PurgeGroupMeta(context.Background(), groupUUID)
	backend.InitGroup(context.Background(), groupUUID, []string{"t1"})

	client := goredis.NewUniversalClient(&goredis.UniversalOptions{Addrs: strings.Split(redisURL, ",")})
	defer client.Close()
	rs := redsync.New(redsyncgoredis.NewPool(client))
	m := rs.NewMutex("TriggerChordMutex")
	require.NoError(t, m.Lock())
	defer m.Unlock()

	_, err := backend.TriggerChord(context.Background(), groupUUID)
	assert.Error(t, err)
}

func TestSetStateRetry(t *testing.T) {
	backend := getRedis()
	if backend == nil {
		t.Skip("REDIS_URL is not defined")
	}

	signature := &tasks.Signature{UUID: "testTaskUUIDRetry", GroupUUID: "testGroupUUIDRetry"}
	backend.PurgeState(context.Background(), signature.UUID)

	err := backend.SetStateRetry(context.Background(), signature)
	assert.NoError(t, err)

	taskState, err := backend.GetState(context.Background(), signature.UUID)
	assert.NoError(t, err)
	assert.Equal(t, signature.Name, taskState.TaskName)
}

func TestUpdateStateMarshalError(t *testing.T) {
	backend := getRedis()
	if backend == nil {
		t.Skip("REDIS_URL is not defined")
	}

	signature := &tasks.Signature{UUID: "testTaskUUIDMarshalErr"}
	defer backend.PurgeState(context.Background(), signature.UUID)

	err := backend.SetStateSuccess(context.Background(), signature, []*tasks.TaskResult{
		{Type: "chan", Value: make(chan int)},
	})
	assert.Error(t, err)
}

// Note: unlike redigo (which forwards "EX <n>" literally and lets Redis
// reject a negative expiration), go-redis's client silently treats a
// negative/zero expiration as "no TTL" and never sends an invalid option to
// Redis. So the conn-level error branches that a negative ResultsExpireIn
// used to exercise in InitGroup/TriggerChord/updateState (and the
// zero-keys MGET error branch in getStates) can't be forced the same way
// here without mocking the client; left uncovered rather than faked.

func TestGetStateDecodeError(t *testing.T) {
	redisURL := getRedisURLString()
	if redisURL == "" {
		t.Skip("REDIS_URL is not defined")
	}

	taskUUID := "testTaskUUIDBadJSON"
	client := goredis.NewUniversalClient(&goredis.UniversalOptions{Addrs: strings.Split(redisURL, ",")})
	defer client.Close()
	defer client.Del(context.Background(), taskUUID)
	assert.NoError(t, client.Set(context.Background(), taskUUID, "not-json", 0).Err())

	backend := getRedis()
	_, err := backend.GetState(context.Background(), taskUUID)
	assert.Error(t, err)
}

func TestGetStatesDecodeError(t *testing.T) {
	redisURL := getRedisURLString()
	if redisURL == "" {
		t.Skip("REDIS_URL is not defined")
	}

	groupUUID := "testGroupUUIDBadState"
	taskUUID := "testTaskUUIDBadState"
	backend := getRedis()
	backend.PurgeGroupMeta(context.Background(), groupUUID)
	backend.PurgeState(context.Background(), taskUUID)
	backend.InitGroup(context.Background(), groupUUID, []string{taskUUID})

	client := goredis.NewUniversalClient(&goredis.UniversalOptions{Addrs: strings.Split(redisURL, ",")})
	defer client.Close()
	defer client.Del(context.Background(), taskUUID)
	assert.NoError(t, client.Set(context.Background(), taskUUID, "not-json", 0).Err())

	_, err := backend.GroupCompleted(context.Background(), groupUUID, 1)
	assert.Error(t, err)
}

func TestGetStatesMissingKeyError(t *testing.T) {
	redisURL := getRedisURLString()
	if redisURL == "" {
		t.Skip("REDIS_URL is not defined")
	}

	groupUUID := "testGroupUUIDMissingState"
	taskUUID := "testTaskUUIDNeverSet"
	backend := getRedis()
	backend.PurgeGroupMeta(context.Background(), groupUUID)
	backend.PurgeState(context.Background(), taskUUID)
	defer backend.PurgeGroupMeta(context.Background(), groupUUID)

	// taskUUID has a group entry but no state was ever set for it, so the
	// pipelined GET comes back with redis.Nil, exercising getStates' per-key
	// error branch (distinct from a decode error).
	backend.InitGroup(context.Background(), groupUUID, []string{taskUUID})

	_, err := backend.GroupTaskStates(context.Background(), groupUUID, 1)
	assert.Error(t, err)
}

func TestGetGroupMetaDecodeError(t *testing.T) {
	redisURL := getRedisURLString()
	if redisURL == "" {
		t.Skip("REDIS_URL is not defined")
	}

	groupUUID := "testGroupUUIDBadMeta"
	client := goredis.NewUniversalClient(&goredis.UniversalOptions{Addrs: strings.Split(redisURL, ",")})
	defer client.Close()
	defer client.Del(context.Background(), groupUUID)
	assert.NoError(t, client.Set(context.Background(), groupUUID, "not-json", 0).Err())

	backend := getRedis()
	_, err := backend.GroupCompleted(context.Background(), groupUUID, 1)
	assert.Error(t, err)
}

func TestNew_AddressParsingVariants(t *testing.T) {
	redisURL := getRedisURLString()
	if redisURL == "" {
		t.Skip("REDIS_URL is not defined")
	}

	cnfWithMaster := new(config.Config)
	cnfWithMaster.Redis = &config.RedisConfig{MasterName: "mymaster"}

	cnfCluster := new(config.Config)
	cnfCluster.Redis = &config.RedisConfig{ClusterMode: true}

	variants := []struct {
		name  string
		cnf   *config.Config
		addrs []string
	}{
		{"plain", new(config.Config), []string{redisURL}},
		{"password-only", new(config.Config), []string{"somepass@" + redisURL}},
		{"user-and-password", new(config.Config), []string{"someuser:somepass@" + redisURL}},
		{"master-name", cnfWithMaster, []string{redisURL}},
		{"cluster-mode", cnfCluster, []string{redisURL}},
	}

	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			backend := redis.New(v.cnf, append([]string(nil), v.addrs...), 0, "")
			assert.NotNil(t, backend)
		})
	}
}
