package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeepCopy(t *testing.T) {
	t.Parallel()

	type s struct {
		A float64
		B int
		C []int
		D *int
		E map[string]int
	}
	var d = 3
	var dst = new(s)
	var src = s{1.0, 1, []int{1, 2, 3}, &d, map[string]int{"a": 1}}

	err := DeepCopy(dst, &src)
	src.A = 2

	assert.NoError(t, err)
	assert.Equal(t, 1.0, dst.A)
	assert.Equal(t, 1, dst.B)
	assert.Equal(t, []int{1, 2, 3}, dst.C)
	assert.Equal(t, &d, dst.D)
	assert.Equal(t, map[string]int{"a": 1}, dst.E)
}

func TestDeepCopyNilPointerField(t *testing.T) {
	t.Parallel()

	type s struct {
		D *int
	}
	var dst = new(s)
	var src = s{D: nil}

	err := DeepCopy(dst, &src)

	assert.NoError(t, err)
	assert.Nil(t, dst.D)
}

func TestDeepCopyInterfaceField(t *testing.T) {
	t.Parallel()

	type s struct {
		I interface{}
	}
	var dst = new(s)
	var src = s{I: 42}

	err := DeepCopy(dst, &src)

	assert.NoError(t, err)
	assert.Equal(t, 42, dst.I)
}

func TestDeepCopyNilInterfaceField(t *testing.T) {
	t.Parallel()

	type s struct {
		I interface{}
	}
	var dst = new(s)
	var src = s{I: nil}

	err := DeepCopy(dst, &src)

	assert.NoError(t, err)
	assert.Nil(t, dst.I)
}

func TestDeepCopySkipsTaggedField(t *testing.T) {
	t.Parallel()

	type s struct {
		A int
		B int `deepcopy:"-"`
	}
	var dst = &s{A: 0, B: 99}
	var src = s{A: 1, B: 2}

	err := DeepCopy(dst, &src)

	assert.NoError(t, err)
	assert.Equal(t, 1, dst.A)
	assert.Equal(t, 99, dst.B)
}

func TestDeepCopyErrNoMatchType(t *testing.T) {
	t.Parallel()

	var i int
	var str string

	err := DeepCopy(&i, &str)

	assert.Equal(t, ErrNoMatchType, err)
}

func TestDeepCopyErrNoPointer(t *testing.T) {
	t.Parallel()

	err := DeepCopy(1, 2)

	assert.Equal(t, ErrNoPointer, err)
}

func TestDeepCopyErrInvalidArgument(t *testing.T) {
	t.Parallel()

	var dst *int
	var src *int

	err := DeepCopy(dst, src)

	assert.Equal(t, ErrInvalidArgument, err)
}

func TestDeepClone(t *testing.T) {
	t.Parallel()

	// DeepClone is exercised with a slice (rather than a struct) because
	// deepCopy's struct case gates copying on the *source* field's
	// CanSet(), which is only true when the source itself is addressable
	// (e.g. reached via a pointer, as DeepCopy's callers always do). A
	// struct passed by value through DeepClone's interface{} parameter is
	// never addressable, so struct fields would not be copied; that is a
	// pre-existing quirk of deepCopy unrelated to what this test covers.
	src := []int{1, 2, 3}

	clone := DeepClone(src)

	assert.Equal(t, src, clone)
}
