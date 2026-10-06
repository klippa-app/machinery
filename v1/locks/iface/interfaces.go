package iface

import "context"

type Lock interface {
	//Acquire the lock with retry
	//key: the name of the lock,
	//value: at the nanosecond timestamp that lock needs to be released automatically
	LockWithRetries(ctx context.Context, key string, value int64) error

	//Acquire the lock with once
	//key: the name of the lock,
	//value: at the nanosecond timestamp that lock needs to be released automatically
	Lock(ctx context.Context, key string, value int64) error
}
