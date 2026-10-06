package common

import (
	"context"
	"net"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/RichardKnop/machinery/v1/config"
)

// NewUniversalOptions builds go-redis UniversalOptions from a list of
// addresses and the library's Redis/TLS configuration, so brokers, backends
// and locks all configure their Redis client identically.
//
// addrs follows the same convention as a redis:// URL with multiple hosts:
// the first entry may have "user:pass@" or "pass@" prepended to it, e.g.
// "pass@host1:port1", "host2:port2". When socketPath is non-empty, it takes
// precedence over addrs and a Unix socket connection is used instead.
func NewUniversalOptions(cnf *config.Config, addrs []string, db int, socketPath string) *redis.UniversalOptions {
	var username, password string
	if len(addrs) > 0 {
		parts := strings.Split(addrs[0], "@")
		if len(parts) >= 2 {
			options := strings.SplitN(strings.Join(parts[:len(parts)-1], "@"), ":", 2)
			if len(options) >= 2 {
				username = options[0]
				password = options[1]
			} else {
				password = options[0]
			}
			addrs[0] = parts[len(parts)-1]
		}
	}

	ropt := &redis.UniversalOptions{
		Addrs:     addrs,
		DB:        db,
		Username:  username,
		Password:  password,
		TLSConfig: cnf.TLSConfig,
	}

	if cnf.Redis != nil {
		r := cnf.Redis
		ropt.MasterName = r.MasterName
		ropt.ClientName = r.ClientName
		if r.MaxIdle > 0 {
			ropt.MaxIdleConns = r.MaxIdle
		}
		if r.MaxActive > 0 {
			ropt.PoolSize = r.MaxActive
		}
		if r.IdleTimeout > 0 {
			ropt.ConnMaxIdleTime = time.Duration(r.IdleTimeout) * time.Second
		}
		if r.ReadTimeout > 0 {
			ropt.ReadTimeout = time.Duration(r.ReadTimeout) * time.Second
		}
		if r.WriteTimeout > 0 {
			ropt.WriteTimeout = time.Duration(r.WriteTimeout) * time.Second
		}
		if r.ConnectTimeout > 0 {
			ropt.DialTimeout = time.Duration(r.ConnectTimeout) * time.Second
		}
	}

	if socketPath != "" {
		ropt.Addrs = []string{socketPath}
		ropt.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socketPath)
		}
	}

	return ropt
}

// NewUniversalClient builds a go-redis client from the given options,
// returning a cluster client when ClusterMode is enabled.
func NewUniversalClient(cnf *config.Config, ropt *redis.UniversalOptions) redis.UniversalClient {
	if cnf.Redis != nil && cnf.Redis.ClusterMode {
		return redis.NewClusterClient(ropt.Cluster())
	}
	return redis.NewUniversalClient(ropt)
}
