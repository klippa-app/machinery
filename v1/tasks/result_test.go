package tasks_test

import (
	"reflect"
	"testing"

	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
)

func TestReflectTaskResults(t *testing.T) {
	t.Parallel()

	taskResults := []*tasks.TaskResult{
		{
			Type:  "[]string",
			Value: []string{"f", "o", "o"},
		},
	}
	results, err := tasks.ReflectTaskResults(taskResults)
	if assert.NoError(t, err) {
		assert.Equal(t, 1, len(results))
		assert.Equal(t, 3, results[0].Len())
		assert.Equal(t, "f", results[0].Index(0).String())
		assert.Equal(t, "o", results[0].Index(1).String())
		assert.Equal(t, "o", results[0].Index(2).String())
	}
}

func TestReflectTaskResultsError(t *testing.T) {
	t.Parallel()

	taskResults := []*tasks.TaskResult{
		{
			Type:  "notarealtype",
			Value: "foo",
		},
	}

	results, err := tasks.ReflectTaskResults(taskResults)
	assert.Error(t, err)
	assert.Nil(t, results)
}

func TestHumanReadableResultsSingle(t *testing.T) {
	t.Parallel()

	results := []reflect.Value{reflect.ValueOf(42)}

	assert.Equal(t, "42", tasks.HumanReadableResults(results))
}

func TestHumanReadableResultsMultiple(t *testing.T) {
	t.Parallel()

	results := []reflect.Value{reflect.ValueOf(42), reflect.ValueOf("foo")}

	assert.Equal(t, "[42, foo]", tasks.HumanReadableResults(results))
}
