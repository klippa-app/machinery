package eager_test

import (
	"context"
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
	triggered, err := backend.TriggerChord(context.Background(), "any-group")
	assert.NoError(t, err)
	assert.True(t, triggered)
}

func TestGetStateNotFound(t *testing.T) {
	backend := eager.New()
	_, err := backend.GetState(context.Background(), "missing-task")
	assert.EqualError(t, err, "Task not found: missing-task")
}

func TestGroupCompletedTaskNotFound(t *testing.T) {
	backend := eager.New()
	require := assert.New(t)

	require.NoError(backend.InitGroup(context.Background(), "group-missing-task", []string{"missing-task"}))

	_, err := backend.GroupCompleted(context.Background(), "group-missing-task", 1)
	require.EqualError(err, "Task not found: missing-task")
}

func TestGroupTaskStatesTaskNotFound(t *testing.T) {
	backend := eager.New()
	require := assert.New(t)

	require.NoError(backend.InitGroup(context.Background(), "group-missing-task-2", []string{"missing-task-2"}))

	_, err := backend.GroupTaskStates(context.Background(), "group-missing-task-2", 1)
	require.EqualError(err, "Task not found: missing-task-2")
}

func TestUpdateStateMarshalError(t *testing.T) {
	backend := eager.New()

	signature := &tasks.Signature{UUID: "bad-result"}
	err := backend.SetStateSuccess(context.Background(), signature, []*tasks.TaskResult{
		{Type: "chan", Value: make(chan int)},
	})
	assert.Error(t, err)
}
