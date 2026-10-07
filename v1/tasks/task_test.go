package tasks_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"

	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
)

func TestTaskCallErrorTest(t *testing.T) {
	t.Parallel()

	// Create test task that returns tasks.ErrRetryTaskLater error
	retriable := func() error { return tasks.NewErrRetryTaskLater("some error", 4*time.Hour) }

	task, err := tasks.New(context.Background(), retriable, []tasks.Arg{})
	assert.NoError(t, err)

	// Invoke TryCall and validate that returned error can be cast to tasks.ErrRetryTaskLater
	results, err := task.Call()
	assert.Nil(t, results)
	assert.NotNil(t, err)
	_, ok := interface{}(err).(tasks.ErrRetryTaskLater)
	assert.True(t, ok, "Error should be castable to tasks.ErrRetryTaskLater")

	// Create test task that returns a standard error
	standard := func() error { return errors.New("some error") }

	task, err = tasks.New(context.Background(), standard, []tasks.Arg{})
	assert.NoError(t, err)

	// Invoke TryCall and validate that returned error is standard
	results, err = task.Call()
	assert.Nil(t, results)
	assert.NotNil(t, err)
	assert.Equal(t, "some error", err.Error())
}

func TestTaskReflectArgs(t *testing.T) {
	t.Parallel()

	task := new(tasks.Task)
	args := []tasks.Arg{
		{
			Type:  "[]int64",
			Value: []int64{1, 2},
		},
	}

	err := task.ReflectArgs(args)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(task.Args))
	assert.Equal(t, "[]int64", task.Args[0].Type().String())
}

func TestTaskCallInvalidArgRobustnessError(t *testing.T) {
	t.Parallel()

	// Create a test task function
	f := func(x int) error { return nil }

	// Construct an invalid argument list and reflect it
	args := []tasks.Arg{
		{Type: "bool", Value: true},
	}

	task, err := tasks.New(context.Background(), f, args)
	assert.NoError(t, err)

	// Invoke TryCall and validate error handling
	results, err := task.Call()
	assert.Equal(t, "reflect: Call using bool as type int", err.Error())
	assert.Nil(t, results)
}

func TestTaskCallInterfaceValuedResult(t *testing.T) {
	t.Parallel()

	// Create a test task function
	f := func() (interface{}, error) { return math.Pi, nil }

	task, err := tasks.New(context.Background(), f, []tasks.Arg{})
	assert.NoError(t, err)

	taskResults, err := task.Call()
	assert.NoError(t, err)
	assert.Equal(t, "float64", taskResults[0].Type)
	assert.Equal(t, math.Pi, taskResults[0].Value)
}

func TestTaskCallWithContext(t *testing.T) {
	t.Parallel()

	f := func(c context.Context) (interface{}, error) {
		assert.NotNil(t, c)
		assert.Nil(t, tasks.SignatureFromContext(c))
		return math.Pi, nil
	}
	task, err := tasks.New(context.Background(), f, []tasks.Arg{})
	assert.NoError(t, err)
	taskResults, err := task.Call()
	assert.NoError(t, err)
	assert.Equal(t, "float64", taskResults[0].Type)
	assert.Equal(t, math.Pi, taskResults[0].Value)
}

func TestTaskCallWithSignatureInContext(t *testing.T) {
	t.Parallel()

	f := func(c context.Context) (interface{}, error) {
		assert.NotNil(t, c)
		signature := tasks.SignatureFromContext(c)
		assert.NotNil(t, signature)
		assert.Equal(t, "foo", signature.Name)
		return math.Pi, nil
	}
	signature, err := tasks.NewSignature("foo", []tasks.Arg{})
	assert.NoError(t, err)
	task, err := tasks.NewWithSignature(context.Background(), f, signature)
	assert.NoError(t, err)
	taskResults, err := task.Call()
	assert.NoError(t, err)
	assert.Equal(t, "float64", taskResults[0].Type)
	assert.Equal(t, math.Pi, taskResults[0].Value)
}

func TestSignatureFromContextNilContext(t *testing.T) {
	t.Parallel()

	assert.Nil(t, tasks.SignatureFromContext(nil))
}

func TestNewReflectArgsError(t *testing.T) {
	t.Parallel()

	f := func() error { return nil }
	badArgs := []tasks.Arg{{Type: "notarealtype", Value: "x"}}

	_, err := tasks.New(context.Background(), f, badArgs)
	assert.Error(t, err)
}

func TestNewWithSignatureReflectArgsError(t *testing.T) {
	t.Parallel()

	f := func() error { return nil }
	signature := &tasks.Signature{
		Name: "foo",
		Args: []tasks.Arg{{Type: "notarealtype", Value: "x"}},
	}

	_, err := tasks.NewWithSignature(context.Background(), f, signature)
	assert.Error(t, err)
}

func TestTaskReflectArgsError(t *testing.T) {
	t.Parallel()

	task := new(tasks.Task)
	badArgs := []tasks.Arg{{Type: "notarealtype", Value: "x"}}

	err := task.ReflectArgs(badArgs)
	assert.Error(t, err)
}

func TestTaskCallNoReturnValue(t *testing.T) {
	t.Parallel()

	f := func() {}
	task, err := tasks.New(context.Background(), f, []tasks.Arg{})
	assert.NoError(t, err)

	results, err := task.Call()
	assert.Nil(t, results)
	assert.Equal(t, tasks.ErrTaskReturnsNoValue, err)
}

func TestTaskCallRetriableResult(t *testing.T) {
	t.Parallel()

	f := func() tasks.Retriable {
		return tasks.NewErrRetryTaskLater("some error", time.Hour)
	}
	task, err := tasks.New(context.Background(), f, []tasks.Arg{})
	assert.NoError(t, err)

	results, err := task.Call()
	assert.Nil(t, results)

	retriable, ok := err.(tasks.ErrRetryTaskLater)
	assert.True(t, ok, "Error should be castable to tasks.ErrRetryTaskLater")
	assert.Equal(t, time.Hour, retriable.RetryIn())
}

type stringerResult struct{}

func (stringerResult) String() string { return "foo" }

func TestTaskCallLastReturnValueNotError(t *testing.T) {
	t.Parallel()

	f := func() fmt.Stringer { return stringerResult{} }
	task, err := tasks.New(context.Background(), f, []tasks.Arg{})
	assert.NoError(t, err)

	results, err := task.Call()
	assert.Nil(t, results)
	assert.Equal(t, tasks.ErrLastReturnValueMustBeError, err)
}

func TestTaskCallPanicWithNonErrorNonStringValue(t *testing.T) {
	t.Parallel()

	f := func() error { panic(42) }
	task, err := tasks.New(context.Background(), f, []tasks.Arg{})
	assert.NoError(t, err)

	results, err := task.Call()
	assert.Nil(t, results)
	assert.Equal(t, tasks.ErrTaskPanicked, err)
}

func TestTaskCallPanicWithErrorValue(t *testing.T) {
	t.Parallel()

	f := func() error { panic(errors.New("boom")) }
	task, err := tasks.New(context.Background(), f, []tasks.Arg{})
	assert.NoError(t, err)

	results, err := task.Call()
	assert.Nil(t, results)
	assert.EqualError(t, err, "boom")
}

func TestTaskCallWithTracingSpan(t *testing.T) {
	t.Parallel()

	tracer := mocktracer.New()
	span := tracer.StartSpan("test")
	ctx := opentracing.ContextWithSpan(context.Background(), span)

	f := func() error { return nil }
	task, err := tasks.New(context.Background(), f, []tasks.Arg{})
	assert.NoError(t, err)
	task.Context = ctx

	results, err := task.Call()
	assert.NoError(t, err)
	assert.NotNil(t, results)
}

func TestTaskCallPanicWithTracingSpan(t *testing.T) {
	t.Parallel()

	tracer := mocktracer.New()
	span := tracer.StartSpan("test")
	ctx := opentracing.ContextWithSpan(context.Background(), span)

	f := func() error { panic("boom") }
	task, err := tasks.New(context.Background(), f, []tasks.Arg{})
	assert.NoError(t, err)
	task.Context = ctx

	results, err := task.Call()
	assert.Nil(t, results)
	assert.EqualError(t, err, "boom")
}
