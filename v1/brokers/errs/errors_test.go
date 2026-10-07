package errs_test

import (
	"errors"
	"testing"

	"github.com/RichardKnop/machinery/v1/brokers/errs"
	"github.com/stretchr/testify/assert"
)

func TestErrCouldNotUnmarshalTaskSignature(t *testing.T) {
	reason := errors.New("invalid JSON")
	err := errs.NewErrCouldNotUnmarshalTaskSignature([]byte(`{"foo"}`), reason)

	assert.Equal(
		t,
		`Could not unmarshal '{"foo"}' into a task signature: invalid JSON`,
		err.Error(),
	)
}

func TestErrConsumerStopped(t *testing.T) {
	assert.EqualError(t, errs.ErrConsumerStopped, "the server has been stopped")
}

func TestErrStopTaskDeletion(t *testing.T) {
	assert.EqualError(t, errs.ErrStopTaskDeletion, "task should not be deleted")
}
