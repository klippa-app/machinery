package tasks_test

import (
	"testing"

	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
)

func TestTaskStateIsCompleted(t *testing.T) {
	t.Parallel()

	taskState := &tasks.TaskState{
		TaskUUID: "taskUUID",
		State:    tasks.StatePending,
	}

	assert.False(t, taskState.IsCompleted())

	taskState.State = tasks.StateReceived
	assert.False(t, taskState.IsCompleted())

	taskState.State = tasks.StateStarted
	assert.False(t, taskState.IsCompleted())

	taskState.State = tasks.StateSuccess
	assert.True(t, taskState.IsCompleted())

	taskState.State = tasks.StateFailure
	assert.True(t, taskState.IsCompleted())
}

func TestNewTaskStateConstructors(t *testing.T) {
	t.Parallel()

	signature := &tasks.Signature{
		UUID: "task_1",
		Name: "foo",
	}

	pending := tasks.NewPendingTaskState(signature)
	assert.Equal(t, "task_1", pending.TaskUUID)
	assert.Equal(t, "foo", pending.TaskName)
	assert.Equal(t, tasks.StatePending, pending.State)

	received := tasks.NewReceivedTaskState(signature)
	assert.Equal(t, "task_1", received.TaskUUID)
	assert.Equal(t, tasks.StateReceived, received.State)

	started := tasks.NewStartedTaskState(signature)
	assert.Equal(t, "task_1", started.TaskUUID)
	assert.Equal(t, tasks.StateStarted, started.State)

	results := []*tasks.TaskResult{{Type: "string", Value: "bar"}}
	success := tasks.NewSuccessTaskState(signature, results)
	assert.Equal(t, "task_1", success.TaskUUID)
	assert.Equal(t, tasks.StateSuccess, success.State)
	assert.Equal(t, results, success.Results)
	assert.True(t, success.IsSuccess())
	assert.False(t, success.IsFailure())

	failure := tasks.NewFailureTaskState(signature, "some error")
	assert.Equal(t, "task_1", failure.TaskUUID)
	assert.Equal(t, tasks.StateFailure, failure.State)
	assert.Equal(t, "some error", failure.Error)
	assert.True(t, failure.IsFailure())
	assert.False(t, failure.IsSuccess())

	retry := tasks.NewRetryTaskState(signature)
	assert.Equal(t, "task_1", retry.TaskUUID)
	assert.Equal(t, tasks.StateRetry, retry.State)
}
