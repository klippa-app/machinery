package machinery

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	neturl "net/url"

	"github.com/RichardKnop/machinery/v1/config"

	eagerbroker "github.com/RichardKnop/machinery/v1/brokers/eager"
	brokeriface "github.com/RichardKnop/machinery/v1/brokers/iface"
	redisbroker "github.com/RichardKnop/machinery/v1/brokers/redis"

	eagerbackend "github.com/RichardKnop/machinery/v1/backends/eager"
	backendiface "github.com/RichardKnop/machinery/v1/backends/iface"
	nullbackend "github.com/RichardKnop/machinery/v1/backends/null"
	redisbackend "github.com/RichardKnop/machinery/v1/backends/redis"

	eagerlock "github.com/RichardKnop/machinery/v1/locks/eager"
	lockiface "github.com/RichardKnop/machinery/v1/locks/iface"
	redislock "github.com/RichardKnop/machinery/v1/locks/redis"
)

// BrokerFactory creates a new object of iface.Broker
// Currently only the Redis broker is supported
func BrokerFactory(cnf *config.Config) (brokeriface.Broker, error) {
	if strings.HasPrefix(cnf.Broker, "redis://") || strings.HasPrefix(cnf.Broker, "rediss://") {
		var scheme string
		if strings.HasPrefix(cnf.Broker, "redis://") {
			scheme = "redis://"
		} else {
			scheme = "rediss://"
		}
		parts := strings.Split(cnf.Broker, scheme)
		if len(parts) != 2 {
			return nil, fmt.Errorf(
				"Redis broker connection string should be in format %shost:port, instead got %s", scheme,
				cnf.Broker,
			)
		}
		brokers := strings.Split(parts[1], ",")
		if len(brokers) > 1 || (cnf.Redis != nil && cnf.Redis.ClusterMode) {
			db, brokers := ParseBrokers(brokers)
			return redisbroker.New(cnf, brokers, db, ""), nil
		} else {
			redisHost, redisUsername, redisPassword, redisDB, err := ParseRedisURL(cnf.Broker)
			if err != nil {
				return nil, err
			}
			addr := buildRedisAddr(redisUsername, redisPassword, redisHost)
			return redisbroker.New(cnf, []string{addr}, redisDB, ""), nil
		}
	}

	if strings.HasPrefix(cnf.Broker, "redis+socket://") {
		redisSocket, redisUsername, redisPassword, redisDB, err := ParseRedisSocketURL(cnf.Broker)
		if err != nil {
			return nil, err
		}

		addr := buildRedisAddr(redisUsername, redisPassword, "")
		return redisbroker.New(cnf, []string{addr}, redisDB, redisSocket), nil
	}

	if strings.HasPrefix(cnf.Broker, "eager") {
		return eagerbroker.New(), nil
	}

	return nil, fmt.Errorf("Factory failed with broker URL: %v", cnf.Broker)
}

// BackendFactory creates a new object of backends.Interface
// Currently supported backends are Redis, eager (in-memory) and null (discard results)
func BackendFactory(cnf *config.Config) (backendiface.Backend, error) {

	if strings.HasPrefix(cnf.ResultBackend, "redis://") || strings.HasPrefix(cnf.ResultBackend, "rediss://") {
		var scheme string
		if strings.HasPrefix(cnf.ResultBackend, "redis://") {
			scheme = "redis://"
		} else {
			scheme = "rediss://"
		}
		parts := strings.Split(cnf.ResultBackend, scheme)
		addrs := strings.Split(parts[1], ",")
		if len(addrs) > 1 || (cnf.Redis != nil && cnf.Redis.ClusterMode) {
			db, addrs := ParseBrokers(addrs)
			return redisbackend.New(cnf, addrs, db, ""), nil
		} else {
			redisHost, redisUsername, redisPassword, redisDB, err := ParseRedisURL(cnf.ResultBackend)

			if err != nil {
				return nil, err
			}

			addr := buildRedisAddr(redisUsername, redisPassword, redisHost)
			return redisbackend.New(cnf, []string{addr}, redisDB, ""), nil
		}
	}

	if strings.HasPrefix(cnf.ResultBackend, "redis+socket://") {
		redisSocket, redisUsername, redisPassword, redisDB, err := ParseRedisSocketURL(cnf.ResultBackend)
		if err != nil {
			return nil, err
		}

		addr := buildRedisAddr(redisUsername, redisPassword, "")
		return redisbackend.New(cnf, []string{addr}, redisDB, redisSocket), nil
	}

	if strings.HasPrefix(cnf.ResultBackend, "eager") {
		return eagerbackend.New(), nil
	}

	if strings.HasPrefix(cnf.ResultBackend, "null") {
		return nullbackend.New(), nil
	}

	return nil, fmt.Errorf("Factory failed with result backend: %v", cnf.ResultBackend)
}

// buildRedisAddr encodes a username/password pair into the "user:pass@host"
// (or "pass@host") convention expected by common.NewUniversalOptions.
func buildRedisAddr(username, password, host string) string {
	switch {
	case username != "":
		return username + ":" + password + "@" + host
	case password != "":
		return password + "@" + host
	default:
		return host
	}
}

// ParseRedisURL ...
func ParseRedisURL(url string) (host, username, password string, db int, err error) {
	// redis://pwd@host/db

	var u *neturl.URL
	u, err = neturl.Parse(url)
	if err != nil {
		return
	}
	if u.Scheme != "redis" && u.Scheme != "rediss" {
		err = errors.New("No redis scheme found")
		return
	}

	if u.User != nil {
		var exists bool
		username = u.User.Username()
		password, exists = u.User.Password()
		if !exists {
			password = u.User.Username()
			username = ""
		}
	}

	host = u.Host

	parts := strings.Split(u.Path, "/")
	if len(parts) == 1 {
		db = 0 //default redis db
	} else {
		db, err = strconv.Atoi(parts[1])
		if err != nil {
			db, err = 0, nil //ignore err here
		}
	}

	return
}

// Parse the database index from the last broker in the list, and remove the /db part from it.
func ParseBrokers(brokers []string) (int, []string) {
	db := 0
	last := brokers[len(brokers)-1]
	if strings.Contains(last, "/") {
		parts := strings.SplitN(last, "/", 2)
		brokers[len(brokers)-1] = parts[0]
		if len(parts) == 2 && parts[1] != "" {
			if val, err := strconv.Atoi(parts[1]); err == nil {
				db = val
			}
		}
	}

	return db, brokers
}

// LockFactory creates a new object of iface.Lock
// Currently supported lock is redis
func LockFactory(cnf *config.Config) (lockiface.Lock, error) {
	if strings.HasPrefix(cnf.Lock, "eager") {
		return eagerlock.New(), nil
	}
	if strings.HasPrefix(cnf.Lock, "redis://") {
		parts := strings.Split(cnf.Lock, "redis://")
		if len(parts) != 2 {
			return nil, fmt.Errorf(
				"Redis broker connection string should be in format redis://host:port, instead got %s",
				cnf.Lock,
			)
		}
		locks := strings.Split(parts[1], ",")
		return redislock.New(cnf, locks, 0, 3), nil
	}

	// Lock is required for periodic tasks to work, therefor return in memory lock in case none is configured
	return eagerlock.New(), nil
}

// ParseRedisSocketURL extracts Redis connection options from a URL with the
// redis+socket:// scheme. This scheme is not standard (or even de facto) and
// is used as a transitional mechanism until the the config package gains the
// proper facilities to support socket-based connections.
func ParseRedisSocketURL(url string) (path, username, password string, db int, err error) {
	parts := strings.Split(url, "redis+socket://")
	if parts[0] != "" {
		err = errors.New("No redis scheme found")
		return
	}

	// redis+socket://password@/path/to/file.soc:/db

	if len(parts) != 2 {
		err = fmt.Errorf("Redis socket connection string should be in format redis+socket://password@/path/to/file.sock:/db, instead got %s", url)
		return
	}

	remainder := parts[1]

	// Extract password if any
	parts = strings.SplitN(remainder, "@", 2)
	if len(parts) == 2 {
		userinfo := strings.SplitN(parts[0], ":", 2)
		if len(userinfo) == 2 {
			username = userinfo[0]
			password = userinfo[1]
		} else {
			password = userinfo[0]
		}
		remainder = parts[1]
	} else {
		remainder = parts[0]
	}

	// Extract path
	parts = strings.SplitN(remainder, ":", 2)
	path = parts[0]
	if path == "" {
		err = fmt.Errorf("Redis socket connection string should be in format redis+socket://password@/path/to/file.sock:/db, instead got %s", url)
		return
	}
	if len(parts) == 2 {
		remainder = parts[1]
	}

	// Extract DB if any
	parts = strings.SplitN(remainder, "/", 2)
	if len(parts) == 2 {
		db, _ = strconv.Atoi(parts[1])
	}

	return
}
