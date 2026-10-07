package machinery_test

import (
	"errors"
	"testing"

	machinery "github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/stretchr/testify/assert"

	redisbroker "github.com/RichardKnop/machinery/v1/brokers/redis"

	redisbackend "github.com/RichardKnop/machinery/v1/backends/redis"
)

var (
	redisSchemeTestCases = []struct {
		desc            string
		url             string
		host, user, pwd string
		db              int
		err             error
	}{
		{
			desc: "invalid redis scheme",
			url:  "non_redis://127.0.0.1:5672",
			err:  errors.New("invalid redis scheme"),
		},
		{
			desc: "empty redis scheme",
			url:  "redis:/",
		},
		{
			desc: "valid scheme but not redis",
			url:  "http://127.0.0.1:5672",
			err:  errors.New("no redis scheme found"),
		},
		{
			desc: "redis host",
			url:  "redis://127.0.0.1:5672",
			host: "127.0.0.1:5672",
		},
		{
			desc: "redis password and host",
			url:  "redis://pwd@127.0.0.1:5672",
			host: "127.0.0.1:5672",
			user: "",
			pwd:  "pwd",
		},
		{
			desc: "redis password, host and db",
			url:  "redis://pwd@127.0.0.1:5672/2",
			host: "127.0.0.1:5672",
			user: "",
			pwd:  "pwd",
			db:   2,
		},
		{
			desc: "redis user, password host",
			url:  "redis://user:pwd@127.0.0.1:5672",
			host: "127.0.0.1:5672",
			user: "user",
			pwd:  "pwd",
		},
		{
			desc: "redis user, password with colon host",
			url:  "redis://user:pwd:with:colon@127.0.0.1:5672",
			host: "127.0.0.1:5672",
			user: "user",
			pwd:  "pwd:with:colon",
		},
		{
			desc: "redis user, empty password and host",
			url:  "redis://user:@127.0.0.1:5672",
			host: "127.0.0.1:5672",
			user: "user",
			pwd:  "",
		},
		{
			desc: "redis empty user, password and host",
			url:  "redis://:pwd@127.0.0.1:5672",
			host: "127.0.0.1:5672",
			user: "",
			pwd:  "pwd",
		},
	}
)

func TestBrokerFactory(t *testing.T) {
	var cnf config.Config

	// Redis broker test

	// with password
	cnf = config.Config{
		Broker:       "redis://password@localhost:6379",
		DefaultQueue: "machinery_tasks",
	}

	actual, err := machinery.BrokerFactory(&cnf)
	if assert.NoError(t, err) {
		_, isRedisBroker := actual.(*redisbroker.Broker)
		assert.True(
			t,
			isRedisBroker,
			"Broker should be instance of *brokers.RedisBroker",
		)
	}

	// with username and password
	cnf = config.Config{
		Broker:       "redis://user:password@localhost:6379",
		DefaultQueue: "machinery_tasks",
	}

	actual, err = machinery.BrokerFactory(&cnf)
	if assert.NoError(t, err) {
		_, isRedisBroker := actual.(*redisbroker.Broker)
		assert.True(
			t,
			isRedisBroker,
			"Broker should be instance of *brokers.RedisBroker",
		)
	}

	// without password
	cnf = config.Config{
		Broker:       "redis://localhost:6379",
		DefaultQueue: "machinery_tasks",
	}

	actual, err = machinery.BrokerFactory(&cnf)
	if assert.NoError(t, err) {
		_, isRedisBroker := actual.(*redisbroker.Broker)
		assert.True(
			t,
			isRedisBroker,
			"Broker should be instance of *brokers.RedisBroker",
		)
	}

	// using a socket file
	cnf = config.Config{
		Broker:       "redis+socket:///tmp/redis.sock",
		DefaultQueue: "machinery_tasks",
	}

	actual, err = machinery.BrokerFactory(&cnf)
	if assert.NoError(t, err) {
		_, isRedisBroker := actual.(*redisbroker.Broker)
		assert.True(
			t,
			isRedisBroker,
			"Broker should be instance of *brokers.RedisBroker",
		)
	}

	// rediss (TLS) scheme
	cnf = config.Config{
		Broker:       "rediss://localhost:6379",
		DefaultQueue: "machinery_tasks",
	}

	actual, err = machinery.BrokerFactory(&cnf)
	if assert.NoError(t, err) {
		_, isRedisBroker := actual.(*redisbroker.Broker)
		assert.True(t, isRedisBroker)
	}

	// multiple brokers (comma separated list) uses the cluster-capable client
	cnf = config.Config{
		Broker:       "redis://host1:6379,host2:6379/3",
		DefaultQueue: "machinery_tasks",
		Redis:        &config.RedisConfig{},
	}

	actual, err = machinery.BrokerFactory(&cnf)
	assert.NoError(t, err)
	assert.NotNil(t, actual)

	// single broker but cluster mode forced through config
	cnf = config.Config{
		Broker:       "redis://host1:6379",
		DefaultQueue: "machinery_tasks",
		Redis:        &config.RedisConfig{ClusterMode: true},
	}

	actual, err = machinery.BrokerFactory(&cnf)
	assert.NoError(t, err)
	assert.NotNil(t, actual)

	// eager broker
	cnf = config.Config{
		Broker:       "eager",
		DefaultQueue: "machinery_tasks",
	}

	actual, err = machinery.BrokerFactory(&cnf)
	assert.NoError(t, err)
	assert.NotNil(t, actual)
}

func TestBrokerFactoryError(t *testing.T) {
	cnf := config.Config{
		Broker: "BOGUS",
	}

	conn, err := machinery.BrokerFactory(&cnf)
	if assert.Error(t, err) {
		assert.Nil(t, conn)
		assert.Equal(t, "Factory failed with broker URL: BOGUS", err.Error())
	}

	// Any non-redis, non-eager broker URL is now rejected (AMQP/SQS/GCPPubSub support was removed)
	cnf = config.Config{
		Broker:       "http://localhost:5672/some-queue",
		DefaultQueue: "machinery_tasks",
	}

	conn, err = machinery.BrokerFactory(&cnf)
	if assert.Error(t, err) {
		assert.Nil(t, conn)
		assert.Equal(t, "Factory failed with broker URL: http://localhost:5672/some-queue", err.Error())
	}

	cnf = config.Config{
		Broker:       "amqp://guest:guest@localhost:5672/",
		DefaultQueue: "machinery_tasks",
	}

	conn, err = machinery.BrokerFactory(&cnf)
	if assert.Error(t, err) {
		assert.Nil(t, conn)
		assert.Equal(t, "Factory failed with broker URL: amqp://guest:guest@localhost:5672/", err.Error())
	}

	// malformed redis connection string (occurs twice, so splitting on the scheme yields more than 2 parts)
	cnf = config.Config{
		Broker:       "redis://redis://",
		DefaultQueue: "machinery_tasks",
	}

	conn, err = machinery.BrokerFactory(&cnf)
	assert.Error(t, err)
	assert.Nil(t, conn)

	// invalid percent-encoding makes url.Parse fail inside ParseRedisURL
	cnf = config.Config{
		Broker:       "redis://%zz",
		DefaultQueue: "machinery_tasks",
	}

	conn, err = machinery.BrokerFactory(&cnf)
	assert.Error(t, err)
	assert.Nil(t, conn)

	// malformed redis socket connection string
	cnf = config.Config{
		Broker:       "redis+socket://",
		DefaultQueue: "machinery_tasks",
	}

	conn, err = machinery.BrokerFactory(&cnf)
	assert.Error(t, err)
	assert.Nil(t, conn)
}

func TestBackendFactory(t *testing.T) {
	t.Parallel()

	var cnf config.Config

	// Redis backend test

	// with password
	cnf = config.Config{
		ResultBackend: "redis://password@localhost:6379",
	}

	actual, err := machinery.BackendFactory(&cnf)
	if assert.NoError(t, err) {
		_, isRedisBackend := actual.(*redisbackend.Backend)
		assert.True(t, isRedisBackend)
	}

	// without password
	cnf = config.Config{
		ResultBackend: "redis://localhost:6379",
	}

	actual, err = machinery.BackendFactory(&cnf)
	if assert.NoError(t, err) {
		_, isRedisBackend := actual.(*redisbackend.Backend)
		assert.True(t, isRedisBackend)
	}

	// using a socket file
	cnf = config.Config{
		ResultBackend: "redis+socket:///tmp/redis.sock",
	}

	actual, err = machinery.BackendFactory(&cnf)
	if assert.NoError(t, err) {
		_, isRedisBackend := actual.(*redisbackend.Backend)
		assert.True(t, isRedisBackend)
	}

	// rediss (TLS) scheme
	cnf = config.Config{ResultBackend: "rediss://localhost:6379"}

	actual, err = machinery.BackendFactory(&cnf)
	assert.NoError(t, err)
	assert.NotNil(t, actual)

	// multiple backends (comma separated list) uses the go-redis cluster client
	cnf = config.Config{ResultBackend: "redis://host1:6379,host2:6379/3"}

	actual, err = machinery.BackendFactory(&cnf)
	assert.NoError(t, err)
	assert.NotNil(t, actual)

	// single backend but cluster mode forced through config
	cnf = config.Config{
		ResultBackend: "redis://host1:6379",
		Redis:         &config.RedisConfig{ClusterMode: true},
	}

	actual, err = machinery.BackendFactory(&cnf)
	assert.NoError(t, err)
	assert.NotNil(t, actual)

	// eager backend
	cnf = config.Config{ResultBackend: "eager"}

	actual, err = machinery.BackendFactory(&cnf)
	assert.NoError(t, err)
	assert.NotNil(t, actual)

	// null backend
	cnf = config.Config{ResultBackend: "null"}

	actual, err = machinery.BackendFactory(&cnf)
	assert.NoError(t, err)
	assert.NotNil(t, actual)
}

func TestBackendFactoryError(t *testing.T) {
	t.Parallel()

	cnf := config.Config{
		ResultBackend: "BOGUS",
	}

	conn, err := machinery.BackendFactory(&cnf)
	if assert.Error(t, err) {
		assert.Nil(t, conn)
		assert.Equal(t, "Factory failed with result backend: BOGUS", err.Error())
	}

	if conn != nil {
		t.Errorf("conn = %v, should be nil", conn)
	}

	// invalid percent-encoding makes url.Parse fail inside ParseRedisURL
	cnf = config.Config{ResultBackend: "redis://%zz"}

	conn, err = machinery.BackendFactory(&cnf)
	assert.Error(t, err)
	assert.Nil(t, conn)

	// malformed redis socket connection string
	cnf = config.Config{ResultBackend: "redis+socket://"}

	conn, err = machinery.BackendFactory(&cnf)
	assert.Error(t, err)
	assert.Nil(t, conn)
}

func TestParseRedisURL(t *testing.T) {
	t.Parallel()

	for _, tc := range redisSchemeTestCases {
		tc := tc // capture range variable
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			host, user, pwd, db, err := machinery.ParseRedisURL(tc.url)
			if tc.err != nil {
				assert.Error(t, err, tc.err)
				return
			}

			if assert.NoError(t, err) {
				assert.Equal(t, tc.host, host)
				assert.Equal(t, tc.user, user)
				assert.Equal(t, tc.pwd, pwd)
				assert.Equal(t, tc.db, db)
			}
		})
	}
}

func TestParseRedisSocketURL(t *testing.T) {
	t.Parallel()

	var (
		path, user, pwd, url string
		db                   int
		err                  error
	)

	url = "non_redissock:///tmp/redis.sock"
	_, _, _, _, err = machinery.ParseRedisSocketURL(url)
	assert.Error(t, err, "invalid redis scheme")

	url = "redis+socket:/"
	_, _, _, _, err = machinery.ParseRedisSocketURL(url)
	assert.Error(t, err, "invalid redis url scheme")

	// scheme occurs twice, so splitting on it yields more than 2 parts
	url = "redis+socket://redis+socket://"
	_, _, _, _, err = machinery.ParseRedisSocketURL(url)
	assert.Error(t, err)

	// empty remainder after the scheme means no path at all
	url = "redis+socket://"
	_, _, _, _, err = machinery.ParseRedisSocketURL(url)
	assert.Error(t, err)

	url = "redis+socket:///tmp/redis.sock"
	path, user, pwd, db, err = machinery.ParseRedisSocketURL(url)
	if assert.NoError(t, err) {
		assert.Equal(t, "/tmp/redis.sock", path)
		assert.Equal(t, "", user)
		assert.Equal(t, "", pwd)
		assert.Equal(t, 0, db)
	}

	url = "redis+socket://pwd@/tmp/redis.sock"
	path, user, pwd, db, err = machinery.ParseRedisSocketURL(url)
	if assert.NoError(t, err) {
		assert.Equal(t, "/tmp/redis.sock", path)
		assert.Equal(t, "", user)
		assert.Equal(t, "pwd", pwd)
		assert.Equal(t, 0, db)
	}

	url = "redis+socket://user:pwd@/tmp/redis.sock"
	path, user, pwd, db, _ = machinery.ParseRedisSocketURL(url)
	if assert.NoError(t, err) {
		assert.Equal(t, "/tmp/redis.sock", path)
		assert.Equal(t, "user", user)
		assert.Equal(t, "pwd", pwd)
		assert.Equal(t, 0, db)
	}

	url = "redis+socket://user:pwd@/tmp/redis.sock:/2"
	path, user, pwd, db, err = machinery.ParseRedisSocketURL(url)
	if assert.NoError(t, err) {
		assert.Equal(t, "/tmp/redis.sock", path)
		assert.Equal(t, "user", user)
		assert.Equal(t, "pwd", pwd)
		assert.Equal(t, 2, db)
	}
}

func TestParseBrokers(t *testing.T) {
	t.Parallel()

	// db suffix present on the last broker
	db, brokers := machinery.ParseBrokers([]string{"host1:6379", "host2:6379/2"})
	assert.Equal(t, 2, db)
	assert.Equal(t, []string{"host1:6379", "host2:6379"}, brokers)

	// db suffix present but empty, falls back to default db 0
	db, brokers = machinery.ParseBrokers([]string{"host1:6379/"})
	assert.Equal(t, 0, db)
	assert.Equal(t, []string{"host1:6379"}, brokers)

	// no db suffix at all
	db, brokers = machinery.ParseBrokers([]string{"host1:6379", "host2:6379"})
	assert.Equal(t, 0, db)
	assert.Equal(t, []string{"host1:6379", "host2:6379"}, brokers)
}

func TestLockFactory(t *testing.T) {
	t.Parallel()

	// eager lock
	cnf := config.Config{Lock: "eager"}
	lock, err := machinery.LockFactory(&cnf)
	assert.NoError(t, err)
	assert.NotNil(t, lock)

	// redis lock
	cnf = config.Config{Lock: "redis://localhost:6379"}
	lock, err = machinery.LockFactory(&cnf)
	assert.NoError(t, err)
	assert.NotNil(t, lock)

	// unrecognized lock falls back to the eager (in-memory) lock
	cnf = config.Config{Lock: ""}
	lock, err = machinery.LockFactory(&cnf)
	assert.NoError(t, err)
	assert.NotNil(t, lock)

	// malformed redis lock connection string
	cnf = config.Config{Lock: "redis://redis://"}
	lock, err = machinery.LockFactory(&cnf)
	assert.Error(t, err)
	assert.Nil(t, lock)
}
