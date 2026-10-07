package null_test

import (
	"context"
	"testing"

	"github.com/RichardKnop/machinery/v1/backends/null"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
)

func TestErrGroupNotFound(t *testing.T) {
	err := null.NewErrGroupNotFound("group1")
	assert.Equal(t, "Group not found: group1", err.Error())
}

func TestErrTasknotFound(t *testing.T) {
	err := null.NewErrTasknotFound("task1")
	assert.Equal(t, "Task not found: task1", err.Error())
}

func TestInitGroup(t *testing.T) {
	backend := null.New()
	assert.Nil(t, backend.InitGroup(context.Background(), "group1", []string{"task1", "task2"}))
}

func TestGroupCompleted(t *testing.T) {
	backend := null.New()

	completed, err := backend.GroupCompleted(context.Background(), "group1", 2)
	assert.False(t, completed)
	assert.Error(t, err)

	assert.Nil(t, backend.InitGroup(context.Background(), "group1", []string{"task1", "task2"}))

	completed, err = backend.GroupCompleted(context.Background(), "group1", 2)
	assert.True(t, completed)
	assert.NoError(t, err)
}

func TestGroupTaskStates(t *testing.T) {
	backend := null.New()

	states, err := backend.GroupTaskStates(context.Background(), "group1", 2)
	assert.Nil(t, states)
	assert.Error(t, err)

	assert.Nil(t, backend.InitGroup(context.Background(), "group1", []string{"task1", "task2"}))

	states, err = backend.GroupTaskStates(context.Background(), "group1", 2)
	assert.NoError(t, err)
	assert.Len(t, states, 0)
}

func TestTriggerChord(t *testing.T) {
	backend := null.New()

	triggered, err := backend.TriggerChord(context.Background(), "group1")
	assert.True(t, triggered)
	assert.NoError(t, err)
}

func TestSetStatePending(t *testing.T) {
	backend := null.New()
	signature := &tasks.Signature{UUID: "task1"}
	assert.NoError(t, backend.SetStatePending(context.Background(), signature))
}

func TestSetStateReceived(t *testing.T) {
	backend := null.New()
	signature := &tasks.Signature{UUID: "task1"}
	assert.NoError(t, backend.SetStateReceived(context.Background(), signature))
}

func TestSetStateStarted(t *testing.T) {
	backend := null.New()
	signature := &tasks.Signature{UUID: "task1"}
	assert.NoError(t, backend.SetStateStarted(context.Background(), signature))
}

func TestSetStateRetry(t *testing.T) {
	backend := null.New()
	signature := &tasks.Signature{UUID: "task1"}
	assert.NoError(t, backend.SetStateRetry(context.Background(), signature))
}

func TestSetStateSuccess(t *testing.T) {
	backend := null.New()
	signature := &tasks.Signature{UUID: "task1"}
	results := []*tasks.TaskResult{{Type: "string", Value: "hi"}}
	assert.NoError(t, backend.SetStateSuccess(context.Background(), signature, results))
}

func TestSetStateFailure(t *testing.T) {
	backend := null.New()
	signature := &tasks.Signature{UUID: "task1"}
	assert.NoError(t, backend.SetStateFailure(context.Background(), signature, "some error"))
}

func TestGetState(t *testing.T) {
	backend := null.New()
	state, err := backend.GetState(context.Background(), "task1")
	assert.Nil(t, state)
	assert.Error(t, err)
}

func TestPurgeState(t *testing.T) {
	backend := null.New()
	assert.Error(t, backend.PurgeState(context.Background(), "task1"))
}

func TestPurgeGroupMeta(t *testing.T) {
	backend := null.New()

	assert.Error(t, backend.PurgeGroupMeta(context.Background(), "group1"))

	assert.Nil(t, backend.InitGroup(context.Background(), "group1", []string{"task1"}))
	assert.NoError(t, backend.PurgeGroupMeta(context.Background(), "group1"))
}
