package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"github.com/go-redsync/redsync/v4"
	redsyncgoredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"

	"github.com/RichardKnop/machinery/v1/backends/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
)

// Backend represents a Redis result backend
type Backend struct {
	common.Backend
	rclient redis.UniversalClient
	redsync *redsync.Redsync
}

// New creates Backend instance. addrs is a list of host:port addresses; the
// first entry may have "user:pass@" or "pass@" prepended to it. When
// socketPath is non-empty, a Unix socket connection is used instead.
func New(cnf *config.Config, addrs []string, db int, socketPath string) iface.Backend {
	b := &Backend{
		Backend: common.NewBackend(cnf),
	}

	ropt := common.NewUniversalOptions(cnf, addrs, db, socketPath)
	b.rclient = common.NewUniversalClient(cnf, ropt)
	b.redsync = redsync.New(redsyncgoredis.NewPool(b.rclient))

	return b
}

// InitGroup creates and saves a group meta data object
func (b *Backend) InitGroup(ctx context.Context, groupUUID string, taskUUIDs []string) error {
	groupMeta := &tasks.GroupMeta{
		GroupUUID: groupUUID,
		TaskUUIDs: taskUUIDs,
		CreatedAt: time.Now().UTC(),
	}

	encoded, err := json.Marshal(groupMeta)
	if err != nil {
		return err
	}

	expiration := b.getExpiration()
	err = b.rclient.Set(ctx, groupUUID, encoded, expiration).Err()
	if err != nil {
		return err
	}

	return nil
}

// GroupCompleted returns true if all tasks in a group finished
func (b *Backend) GroupCompleted(ctx context.Context, groupUUID string, groupTaskCount int) (bool, error) {
	groupMeta, err := b.getGroupMeta(ctx, groupUUID)
	if err != nil {
		return false, err
	}

	taskStates, err := b.getStates(ctx, groupMeta.TaskUUIDs...)
	if err != nil {
		return false, err
	}

	var countSuccessTasks = 0
	for _, taskState := range taskStates {
		if taskState.IsCompleted() {
			countSuccessTasks++
		}
	}

	return countSuccessTasks == groupTaskCount, nil
}

// GroupTaskStates returns states of all tasks in the group
func (b *Backend) GroupTaskStates(ctx context.Context, groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error) {
	groupMeta, err := b.getGroupMeta(ctx, groupUUID)
	if err != nil {
		return []*tasks.TaskState{}, err
	}

	return b.getStates(ctx, groupMeta.TaskUUIDs...)
}

// TriggerChord flags chord as triggered in the backend storage to make sure
// chord is never trigerred multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *Backend) TriggerChord(ctx context.Context, groupUUID string) (bool, error) {
	m := b.redsync.NewMutex("TriggerChordMutex")
	if err := m.Lock(); err != nil {
		return false, err
	}
	defer m.Unlock()

	groupMeta, err := b.getGroupMeta(ctx, groupUUID)
	if err != nil {
		return false, err
	}

	// Chord has already been triggered, return false (should not trigger again)
	if groupMeta.ChordTriggered {
		return false, nil
	}

	// Set flag to true
	groupMeta.ChordTriggered = true

	// Update the group meta
	encoded, err := json.Marshal(&groupMeta)
	if err != nil {
		return false, err
	}

	expiration := b.getExpiration()
	err = b.rclient.Set(ctx, groupUUID, encoded, expiration).Err()
	if err != nil {
		return false, err
	}

	return true, nil
}

func (b *Backend) mergeNewTaskState(ctx context.Context, newState *tasks.TaskState) {
	state, err := b.GetState(ctx, newState.TaskUUID)
	if err == nil {
		newState.CreatedAt = state.CreatedAt
		newState.TaskName = state.TaskName
	}
}

// SetStatePending updates task state to PENDING
func (b *Backend) SetStatePending(ctx context.Context, signature *tasks.Signature) error {
	taskState := tasks.NewPendingTaskState(signature)
	return b.updateState(ctx, taskState)
}

// SetStateReceived updates task state to RECEIVED
func (b *Backend) SetStateReceived(ctx context.Context, signature *tasks.Signature) error {
	taskState := tasks.NewReceivedTaskState(signature)
	b.mergeNewTaskState(ctx, taskState)
	return b.updateState(ctx, taskState)
}

// SetStateStarted updates task state to STARTED
func (b *Backend) SetStateStarted(ctx context.Context, signature *tasks.Signature) error {
	taskState := tasks.NewStartedTaskState(signature)
	b.mergeNewTaskState(ctx, taskState)
	return b.updateState(ctx, taskState)
}

// SetStateRetry updates task state to RETRY
func (b *Backend) SetStateRetry(ctx context.Context, signature *tasks.Signature) error {
	taskState := tasks.NewRetryTaskState(signature)
	b.mergeNewTaskState(ctx, taskState)
	return b.updateState(ctx, taskState)
}

// SetStateSuccess updates task state to SUCCESS
func (b *Backend) SetStateSuccess(ctx context.Context, signature *tasks.Signature, results []*tasks.TaskResult) error {
	taskState := tasks.NewSuccessTaskState(signature, results)
	b.mergeNewTaskState(ctx, taskState)
	return b.updateState(ctx, taskState)
}

// SetStateFailure updates task state to FAILURE
func (b *Backend) SetStateFailure(ctx context.Context, signature *tasks.Signature, err string) error {
	taskState := tasks.NewFailureTaskState(signature, err)
	b.mergeNewTaskState(ctx, taskState)
	return b.updateState(ctx, taskState)
}

// GetState returns the latest task state
func (b *Backend) GetState(ctx context.Context, taskUUID string) (*tasks.TaskState, error) {
	item, err := b.rclient.Get(ctx, taskUUID).Bytes()
	if err != nil {
		return nil, err
	}
	state := new(tasks.TaskState)
	decoder := json.NewDecoder(bytes.NewReader(item))
	decoder.UseNumber()
	if err := decoder.Decode(state); err != nil {
		return nil, err
	}

	return state, nil
}

// PurgeState deletes stored task state
func (b *Backend) PurgeState(ctx context.Context, taskUUID string) error {
	return b.rclient.Del(ctx, taskUUID).Err()
}

// PurgeGroupMeta deletes stored group meta data
func (b *Backend) PurgeGroupMeta(ctx context.Context, groupUUID string) error {
	return b.rclient.Del(ctx, groupUUID).Err()
}

// getGroupMeta retrieves group meta data, convenience function to avoid repetition
func (b *Backend) getGroupMeta(ctx context.Context, groupUUID string) (*tasks.GroupMeta, error) {
	item, err := b.rclient.Get(ctx, groupUUID).Bytes()
	if err != nil {
		return nil, err
	}

	groupMeta := new(tasks.GroupMeta)
	decoder := json.NewDecoder(bytes.NewReader(item))
	decoder.UseNumber()
	if err := decoder.Decode(groupMeta); err != nil {
		return nil, err
	}

	return groupMeta, nil
}

// getStates returns multiple task states
func (b *Backend) getStates(ctx context.Context, taskUUIDs ...string) ([]*tasks.TaskState, error) {
	taskStates := make([]*tasks.TaskState, len(taskUUIDs))
	// to avoid CROSSSLOT error, use pipeline
	cmders, err := b.rclient.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		for _, uuid := range taskUUIDs {
			pipeliner.Get(ctx, uuid)
		}
		return nil
	})
	if err != nil {
		return taskStates, err
	}
	for i, cmder := range cmders {
		stateBytes, err1 := cmder.(*redis.StringCmd).Bytes()
		if err1 != nil {
			return taskStates, err1
		}
		taskState := new(tasks.TaskState)
		decoder := json.NewDecoder(bytes.NewReader(stateBytes))
		decoder.UseNumber()
		if err1 = decoder.Decode(taskState); err1 != nil {
			log.ERROR.Print(err1)
			return taskStates, err1
		}
		taskStates[i] = taskState
	}

	return taskStates, nil
}

// updateState saves current task state
func (b *Backend) updateState(ctx context.Context, taskState *tasks.TaskState) error {
	encoded, err := json.Marshal(taskState)
	if err != nil {
		return err
	}

	expiration := b.getExpiration()
	_, err = b.rclient.Set(ctx, taskState.TaskUUID, encoded, expiration).Result()
	if err != nil {
		return err
	}

	return nil
}

// getExpiration returns expiration for a stored task state
func (b *Backend) getExpiration() time.Duration {
	expiresIn := b.GetConfig().ResultsExpireIn
	if expiresIn == 0 {
		// expire results after 1 hour by default
		expiresIn = config.DefaultResultsExpireIn
	}

	return time.Duration(expiresIn) * time.Second
}
