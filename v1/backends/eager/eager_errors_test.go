package eager_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/RichardKnop/machinery/v1/backends/eager"
	"github.com/RichardKnop/machinery/v1/tasks"
)

func TestErrGroupNotFoundError(t *testing.T) {
	err := eager.NewErrGroupNotFound("group1")
	assert.EqualError(t, err, "Group not found: group1")
}

func TestErrTasknotFoundError(t *testing.T) {
	err := eager.NewErrTasknotFound("task1")
	assert.EqualError(t, err, "Task not found: task1")
}

func TestTriggerChord(t *testing.T) {
	backend := eager.New()
	triggered, err := backend.TriggerChord("any-group")
	assert.NoError(t, err)
	assert.True(t, triggered)
}

func TestGetStateNotFound(t *testing.T) {
	backend := eager.New()
	_, err := backend.GetState("missing-task")
	assert.EqualError(t, err, "Task not found: missing-task")
}

func TestGroupCompletedTaskNotFound(t *testing.T) {
	backend := eager.New()
	require := assert.New(t)

	require.NoError(backend.InitGroup("group-missing-task", []string{"missing-task"}))

	_, err := backend.GroupCompleted("group-missing-task", 1)
	require.EqualError(err, "Task not found: missing-task")
}

func TestGroupTaskStatesTaskNotFound(t *testing.T) {
	backend := eager.New()
	require := assert.New(t)

	require.NoError(backend.InitGroup("group-missing-task-2", []string{"missing-task-2"}))

	_, err := backend.GroupTaskStates("group-missing-task-2", 1)
	require.EqualError(err, "Task not found: missing-task-2")
}

func TestUpdateStateMarshalError(t *testing.T) {
	backend := eager.New()

	signature := &tasks.Signature{UUID: "bad-result"}
	err := backend.SetStateSuccess(signature, []*tasks.TaskResult{
		{Type: "chan", Value: make(chan int)},
	})
	assert.Error(t, err)
}
