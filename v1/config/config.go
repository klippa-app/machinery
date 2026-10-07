package config

import (
	"crypto/tls"
	"time"
)

const (
	// DefaultResultsExpireIn is a default time used to expire task states and group metadata from the backend
	DefaultResultsExpireIn = 3600
)

var (
	// Start with sensible default values
	defaultCnf = &Config{
		Broker:          "redis://127.0.0.1:6379",
		DefaultQueue:    "machinery_tasks",
		ResultBackend:   "redis://127.0.0.1:6379",
		ResultsExpireIn: DefaultResultsExpireIn,
		Redis: &RedisConfig{
			MaxIdle:                3,
			IdleTimeout:            240,
			ReadTimeout:            15,
			WriteTimeout:           15,
			ConnectTimeout:         15,
			NormalTasksPollPeriod:  1000,
			DelayedTasksPollPeriod: 500,
		},
	}

	reloadDelay = time.Second * 10
)

// Config holds all configuration for our program
type Config struct {
	Broker                  string       `yaml:"broker" envconfig:"BROKER"`
	Lock                    string       `yaml:"lock" envconfig:"LOCK"`
	MultipleBrokerSeparator string       `yaml:"multiple_broker_separator" envconfig:"MULTIPLE_BROKEN_SEPARATOR"`
	DefaultQueue            string       `yaml:"default_queue" envconfig:"DEFAULT_QUEUE"`
	ResultBackend           string       `yaml:"result_backend" envconfig:"RESULT_BACKEND"`
	ResultsExpireIn         int          `yaml:"results_expire_in" envconfig:"RESULTS_EXPIRE_IN"`
	Redis                   *RedisConfig `yaml:"redis"`
	TLSConfig               *tls.Config
	// NoUnixSignals - when set disables signal handling in machinery
	NoUnixSignals bool `yaml:"no_unix_signals" envconfig:"NO_UNIX_SIGNALS"`
}

// RedisConfig ...
type RedisConfig struct {
	// Maximum number of idle connections in the pool.
	// Default: 10
	MaxIdle int `yaml:"max_idle" envconfig:"REDIS_MAX_IDLE"`

	// Maximum number of connections allocated by the pool at a given time.
	// When zero, there is no limit on the number of connections in the pool.
	// Default: 100
	MaxActive int `yaml:"max_active" envconfig:"REDIS_MAX_ACTIVE"`

	// Close connections after remaining idle for this duration in seconds. If the value
	// is zero, then idle connections are not closed. Applications should set
	// the timeout to a value less than the server's timeout.
	// Default: 300
	IdleTimeout int `yaml:"max_idle_timeout" envconfig:"REDIS_IDLE_TIMEOUT"`

	// If Wait is true and the pool is at the MaxActive limit, then Get() waits
	// for a connection to be returned to the pool before returning.
	// Default: true
	Wait bool `yaml:"wait" envconfig:"REDIS_WAIT"`

	// ReadTimeout specifies the timeout in seconds for reading a single command reply.
	// Default: 15
	ReadTimeout int `yaml:"read_timeout" envconfig:"REDIS_READ_TIMEOUT"`

	// WriteTimeout specifies the timeout in seconds for writing a single command.
	// Default: 15
	WriteTimeout int `yaml:"write_timeout" envconfig:"REDIS_WRITE_TIMEOUT"`

	// ConnectTimeout specifies the timeout in seconds for connecting to the Redis server when
	// no DialNetDial option is specified.
	// Default: 15
	ConnectTimeout int `yaml:"connect_timeout" envconfig:"REDIS_CONNECT_TIMEOUT"`

	// NormalTasksPollPeriod specifies the period in milliseconds when polling redis for normal tasks
	// Default: 1000
	NormalTasksPollPeriod int `yaml:"normal_tasks_poll_period" envconfig:"REDIS_NORMAL_TASKS_POLL_PERIOD"`

	// DelayedTasksPollPeriod specifies the period in milliseconds when polling redis for delayed tasks
	// Default: 20
	DelayedTasksPollPeriod int    `yaml:"delayed_tasks_poll_period" envconfig:"REDIS_DELAYED_TASKS_POLL_PERIOD"`
	DelayedTasksKey        string `yaml:"delayed_tasks_key" envconfig:"REDIS_DELAYED_TASKS_KEY"`

	// ClientName specifies the redis client name to be set when connecting to the Redis server
	ClientName string `yaml:"client_name" envconfig:"REDIS_CLIENT_NAME"`

	// MasterName specifies a redis master name in order to configure a sentinel-backed redis FailoverClient
	MasterName string `yaml:"master_name" envconfig:"REDIS_MASTER_NAME"`
	// ClusterMode specifies machinery should use redis cluster client explicitly
	ClusterMode bool `yaml:"cluster_mode" envconfig:"REDIS_CLUSTER_MODE"`

	// Partitioning enables per-partition-key concurrency limits for this
	// broker's queue (see tasks.Signature.PartitionKey). Nil (the default)
	// disables partitioning entirely: PartitionKey is ignored and every task
	// shares the plain queue exactly as before.
	Partitioning *PartitioningConfig `yaml:"partitioning"`
}

// PartitioningConfig configures per-partition-key concurrency limits, so
// that a backlog of tasks sharing one partition key (e.g. a customer or
// tenant ID) cannot starve tasks in other partitions. Modeled after River
// Pro's concurrency limits: https://riverqueue.com/docs/pro/concurrency-limits
//
// Note on latency for brand-new partition keys: a worker blocked waiting
// for work (BLPOP) only watches the partition keys it already knew about
// when that wait began. Redis wakes a blocked BLPOP as soon as any watched
// key receives a push, but a key it isn't watching yet won't wake it. So
// the very first task published under a partition key a worker hasn't seen
// before may sit for up to one NormalTasksPollPeriod before being picked
// up, even though every later task for that same key is dispatched
// immediately. Unpartitioned tasks and already-seen partition keys are
// unaffected.
type PartitioningConfig struct {
	// GlobalConcurrency caps how many tasks sharing a partition key may be
	// processed at once across every worker process sharing this Redis
	// instance. Zero (the default) means no global cap - LocalConcurrency,
	// if set, still applies.
	//
	// Enforced via a self-expiring lease per in-flight task rather than a
	// plain counter, so a worker process that dies or panics mid-task
	// doesn't leak that partition's concurrency slot forever - see
	// LeaseTimeout.
	GlobalConcurrency int `yaml:"global_concurrency" envconfig:"REDIS_PARTITION_GLOBAL_CONCURRENCY"`

	// LocalConcurrency caps how many tasks sharing a partition key may be
	// processed at once by this single worker process. Zero (the default)
	// means no local cap - GlobalConcurrency, if set, still applies.
	// Enforced in-memory; free of any Redis round trip.
	LocalConcurrency int `yaml:"local_concurrency" envconfig:"REDIS_PARTITION_LOCAL_CONCURRENCY"`

	// LeaseTimeout bounds how long a global concurrency slot (see
	// GlobalConcurrency) may be held before it is automatically reclaimed,
	// in case the worker holding it died or panicked without releasing it.
	// A task that legitimately runs longer than this will have its slot
	// reclaimed anyway, so set this comfortably above your longest expected
	// task duration. Defaults to 1 hour when zero.
	LeaseTimeout time.Duration `yaml:"lease_timeout" envconfig:"REDIS_PARTITION_LEASE_TIMEOUT"`
}
