package tasks_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
)

func TestHeadersSet(t *testing.T) {
	t.Parallel()

	h := tasks.Headers{}
	h.Set("key", "value")
	assert.Equal(t, "value", h["key"])
}

func TestHeadersForeachKey(t *testing.T) {
	t.Parallel()

	h := tasks.Headers{
		"a": "1",
		"b": 2, // non-string values should be skipped
	}

	seen := map[string]string{}
	err := h.ForeachKey(func(key, val string) error {
		seen[key] = val
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "1"}, seen)
}

func TestHeadersForeachKeyHandlerError(t *testing.T) {
	t.Parallel()

	h := tasks.Headers{"a": "1"}
	expectedErr := errors.New("handler failed")

	err := h.ForeachKey(func(key, val string) error {
		return expectedErr
	})

	assert.Equal(t, expectedErr, err)
}

func TestNewSignature(t *testing.T) {
	t.Parallel()

	sig, err := tasks.NewSignature("foo", []tasks.Arg{})
	assert.NoError(t, err)
	assert.Equal(t, "foo", sig.Name)
	assert.True(t, strings.HasPrefix(sig.UUID, "task_"))
}

func TestCopySignature(t *testing.T) {
	t.Parallel()

	sig := &tasks.Signature{
		Name: "foo",
		UUID: "task_1",
	}

	copySig := tasks.CopySignature(sig)

	assert.Equal(t, sig.Name, copySig.Name)
	assert.Equal(t, sig.UUID, copySig.UUID)
	assert.NotSame(t, sig, copySig)
}

func TestCopySignatures(t *testing.T) {
	t.Parallel()

	sig1 := &tasks.Signature{Name: "foo"}
	sig2 := &tasks.Signature{Name: "bar"}

	copies := tasks.CopySignatures(sig1, sig2)

	assert.Len(t, copies, 2)
	assert.Equal(t, "foo", copies[0].Name)
	assert.Equal(t, "bar", copies[1].Name)
	assert.NotSame(t, sig1, copies[0])
	assert.NotSame(t, sig2, copies[1])
}

func TestSignatureToJSONByte(t *testing.T) {
	t.Parallel()

	sig := tasks.Signature{Name: "foo"}

	b, err := sig.ToJSONByte()

	assert.NoError(t, err)
	assert.Contains(t, string(b), `"Name":"foo"`)
}
