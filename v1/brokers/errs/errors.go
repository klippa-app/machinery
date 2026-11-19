package errs

import (
	"errors"
	"fmt"
)

// ErrCouldNotUnmarshalTaskSignature ...
type ErrCouldNotUnmarshalTaskSignature struct {
	msg    []byte
	reason string
}

// Error implements the error interface
func (e ErrCouldNotUnmarshalTaskSignature) Error() string {
	return fmt.Sprintf("Could not unmarshal '%s' into a task signature: %v", e.msg, e.reason)
}

// NewErrCouldNotUnmarshalTaskSignature returns new ErrCouldNotUnmarshalTaskSignature instance
func NewErrCouldNotUnmarshalTaskSignature(msg []byte, err error) ErrCouldNotUnmarshalTaskSignature {
	return ErrCouldNotUnmarshalTaskSignature{msg: msg, reason: err.Error()}
}

// ErrConsumerStopped indicates that the operation is now illegal because of the consumer being stopped.
var ErrConsumerStopped = errors.New("the server has been stopped")

// ErrStopTaskDeletion indicates that the task should not be deleted from source after task failure
var ErrStopTaskDeletion = errors.New("task should not be deleted")

// ErrTaskNotStarted indicates that a task was received but never ran, so it can safely be requeued.
var ErrTaskNotStarted = errors.New("task not started")

// ErrParkedTaskNotFound indicates that no parked task has the given UUID.
var ErrParkedTaskNotFound = errors.New("parked task not found")
