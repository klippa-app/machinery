package iface

import (
	"context"

	"github.com/RichardKnop/machinery/v1/tasks"
)

// Backend - a common interface for all result backends
type Backend interface {
	// Group related functions
	InitGroup(ctx context.Context, groupUUID string, taskUUIDs []string) error
	GroupCompleted(ctx context.Context, groupUUID string, groupTaskCount int) (bool, error)
	GroupTaskStates(ctx context.Context, groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error)
	TriggerChord(ctx context.Context, groupUUID string) (bool, error)

	// Setting / getting task state
	SetStatePending(ctx context.Context, signature *tasks.Signature) error
	SetStateReceived(ctx context.Context, signature *tasks.Signature) error
	SetStateStarted(ctx context.Context, signature *tasks.Signature) error
	SetStateRetry(ctx context.Context, signature *tasks.Signature) error
	SetStateSuccess(ctx context.Context, signature *tasks.Signature, results []*tasks.TaskResult) error
	SetStateFailure(ctx context.Context, signature *tasks.Signature, err string) error
	GetState(ctx context.Context, taskUUID string) (*tasks.TaskState, error)

	// Purging stored stored tasks states and group meta data
	PurgeState(ctx context.Context, taskUUID string) error
	PurgeGroupMeta(ctx context.Context, groupUUID string) error
}
