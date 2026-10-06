package null_test

import (
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
	assert.Nil(t, backend.InitGroup("group1", []string{"task1", "task2"}))
}

func TestGroupCompleted(t *testing.T) {
	backend := null.New()

	completed, err := backend.GroupCompleted("group1", 2)
	assert.False(t, completed)
	assert.Error(t, err)

	assert.Nil(t, backend.InitGroup("group1", []string{"task1", "task2"}))

	completed, err = backend.GroupCompleted("group1", 2)
	assert.True(t, completed)
	assert.NoError(t, err)
}

func TestGroupTaskStates(t *testing.T) {
	backend := null.New()

	states, err := backend.GroupTaskStates("group1", 2)
	assert.Nil(t, states)
	assert.Error(t, err)

	assert.Nil(t, backend.InitGroup("group1", []string{"task1", "task2"}))

	states, err = backend.GroupTaskStates("group1", 2)
	assert.NoError(t, err)
	assert.Len(t, states, 0)
}

func TestTriggerChord(t *testing.T) {
	backend := null.New()

	triggered, err := backend.TriggerChord("group1")
	assert.True(t, triggered)
	assert.NoError(t, err)
}

func TestSetStatePending(t *testing.T) {
	backend := null.New()
	signature := &tasks.Signature{UUID: "task1"}
	assert.NoError(t, backend.SetStatePending(signature))
}

func TestSetStateReceived(t *testing.T) {
	backend := null.New()
	signature := &tasks.Signature{UUID: "task1"}
	assert.NoError(t, backend.SetStateReceived(signature))
}

func TestSetStateStarted(t *testing.T) {
	backend := null.New()
	signature := &tasks.Signature{UUID: "task1"}
	assert.NoError(t, backend.SetStateStarted(signature))
}

func TestSetStateRetry(t *testing.T) {
	backend := null.New()
	signature := &tasks.Signature{UUID: "task1"}
	assert.NoError(t, backend.SetStateRetry(signature))
}

func TestSetStateSuccess(t *testing.T) {
	backend := null.New()
	signature := &tasks.Signature{UUID: "task1"}
	results := []*tasks.TaskResult{{Type: "string", Value: "hi"}}
	assert.NoError(t, backend.SetStateSuccess(signature, results))
}

func TestSetStateFailure(t *testing.T) {
	backend := null.New()
	signature := &tasks.Signature{UUID: "task1"}
	assert.NoError(t, backend.SetStateFailure(signature, "some error"))
}

func TestGetState(t *testing.T) {
	backend := null.New()
	state, err := backend.GetState("task1")
	assert.Nil(t, state)
	assert.Error(t, err)
}

func TestPurgeState(t *testing.T) {
	backend := null.New()
	assert.Error(t, backend.PurgeState("task1"))
}

func TestPurgeGroupMeta(t *testing.T) {
	backend := null.New()

	assert.Error(t, backend.PurgeGroupMeta("group1"))

	assert.Nil(t, backend.InitGroup("group1", []string{"task1"}))
	assert.NoError(t, backend.PurgeGroupMeta("group1"))
}
