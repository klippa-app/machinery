package iface

import (
	"context"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
)

// Broker - a common interface for all brokers
type Broker interface {
	GetConfig() *config.Config
	SetRegisteredTaskNames(names []string)
	IsTaskRegistered(name string) bool
	StartConsuming(consumerTag string, concurrency int, p TaskProcessor) (bool, error)
	StopConsuming()
	Publish(ctx context.Context, task *tasks.Signature) error
	GetPendingTasks(ctx context.Context, queue string) ([]*tasks.Signature, error)
	GetPendingTaskCount(ctx context.Context, queue string) (int64, error)
	GetDelayedTasks(ctx context.Context) ([]*tasks.Signature, error)
	CancelDelayedTask(ctx context.Context, taskUUID string) error
	AdjustRoutingKey(s *tasks.Signature)
}

// TaskProcessor - can process a delivered task
// This will probably always be a worker instance
type TaskProcessor interface {
	Process(ctx context.Context, signature *tasks.Signature) error
	CustomQueue() string
	PreConsumeHandler() bool
}
