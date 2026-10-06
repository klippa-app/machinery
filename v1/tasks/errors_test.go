package tasks_test

import (
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
)

func TestErrRetryTaskLater(t *testing.T) {
	t.Parallel()

	err := tasks.NewErrRetryTaskLater("some error", 4*time.Hour)

	assert.Equal(t, 4*time.Hour, err.RetryIn())
	assert.Equal(t, "Task error: some error Will retry in: 4h0m0s", err.Error())

	var retriable tasks.Retriable = err
	assert.Equal(t, 4*time.Hour, retriable.RetryIn())
}
