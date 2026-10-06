package common_test

import (
	"crypto/tls"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
)

func TestNewUniversalOptions_PlainAddr(t *testing.T) {
	cnf := &config.Config{}
	ropt := common.NewUniversalOptions(cnf, []string{"localhost:6379"}, 2, "")

	assert.Equal(t, []string{"localhost:6379"}, ropt.Addrs)
	assert.Equal(t, 2, ropt.DB)
	assert.Equal(t, "", ropt.Username)
	assert.Equal(t, "", ropt.Password)
}

func TestNewUniversalOptions_PasswordOnly(t *testing.T) {
	cnf := &config.Config{}
	addrs := []string{"secret@localhost:6379"}
	ropt := common.NewUniversalOptions(cnf, addrs, 0, "")

	assert.Equal(t, "", ropt.Username)
	assert.Equal(t, "secret", ropt.Password)
	assert.Equal(t, []string{"localhost:6379"}, ropt.Addrs)
}

func TestNewUniversalOptions_UsernameAndPassword(t *testing.T) {
	cnf := &config.Config{}
	addrs := []string{"user:secret@localhost:6379"}
	ropt := common.NewUniversalOptions(cnf, addrs, 0, "")

	assert.Equal(t, "user", ropt.Username)
	assert.Equal(t, "secret", ropt.Password)
	assert.Equal(t, []string{"localhost:6379"}, ropt.Addrs)
}

func TestNewUniversalOptions_MultipleAddrs(t *testing.T) {
	cnf := &config.Config{}
	addrs := []string{"secret@host1:6379", "host2:6379"}
	ropt := common.NewUniversalOptions(cnf, addrs, 0, "")

	assert.Equal(t, "secret", ropt.Password)
	assert.Equal(t, []string{"host1:6379", "host2:6379"}, ropt.Addrs)
}

func TestNewUniversalOptions_EmptyAddrs(t *testing.T) {
	cnf := &config.Config{}
	ropt := common.NewUniversalOptions(cnf, nil, 0, "")

	assert.Equal(t, "", ropt.Username)
	assert.Equal(t, "", ropt.Password)
}

func TestNewUniversalOptions_TLS(t *testing.T) {
	tlsConfig := &tls.Config{}
	cnf := &config.Config{TLSConfig: tlsConfig}
	ropt := common.NewUniversalOptions(cnf, []string{"localhost:6379"}, 0, "")

	assert.Same(t, tlsConfig, ropt.TLSConfig)
}

func TestNewUniversalOptions_RedisConfigTuning(t *testing.T) {
	cnf := &config.Config{
		Redis: &config.RedisConfig{
			MasterName:     "mymaster",
			ClientName:     "my-client",
			MaxIdle:        5,
			MaxActive:      50,
			IdleTimeout:    30,
			ReadTimeout:    10,
			WriteTimeout:   11,
			ConnectTimeout: 12,
		},
	}
	ropt := common.NewUniversalOptions(cnf, []string{"localhost:6379"}, 0, "")

	assert.Equal(t, "mymaster", ropt.MasterName)
	assert.Equal(t, "my-client", ropt.ClientName)
	assert.Equal(t, 5, ropt.MaxIdleConns)
	assert.Equal(t, 50, ropt.PoolSize)
	assert.Equal(t, 30*time.Second, ropt.ConnMaxIdleTime)
	assert.Equal(t, 10*time.Second, ropt.ReadTimeout)
	assert.Equal(t, 11*time.Second, ropt.WriteTimeout)
	assert.Equal(t, 12*time.Second, ropt.DialTimeout)
}

func TestNewUniversalOptions_NilRedisConfig(t *testing.T) {
	cnf := &config.Config{}
	ropt := common.NewUniversalOptions(cnf, []string{"localhost:6379"}, 0, "")

	assert.Equal(t, "", ropt.MasterName)
	assert.Equal(t, 0, ropt.MaxIdleConns)
}

func TestNewUniversalOptions_ZeroTuningValuesIgnored(t *testing.T) {
	cnf := &config.Config{
		Redis: &config.RedisConfig{},
	}
	ropt := common.NewUniversalOptions(cnf, []string{"localhost:6379"}, 0, "")

	assert.Equal(t, 0, ropt.MaxIdleConns)
	assert.Equal(t, 0, ropt.PoolSize)
	assert.Equal(t, time.Duration(0), ropt.ConnMaxIdleTime)
	assert.Equal(t, time.Duration(0), ropt.ReadTimeout)
	assert.Equal(t, time.Duration(0), ropt.WriteTimeout)
	assert.Equal(t, time.Duration(0), ropt.DialTimeout)
}

func TestNewUniversalOptions_SocketPath(t *testing.T) {
	cnf := &config.Config{}
	ropt := common.NewUniversalOptions(cnf, []string{"localhost:6379"}, 0, "/tmp/redis.sock")

	assert.Equal(t, []string{"/tmp/redis.sock"}, ropt.Addrs)
	assert.NotNil(t, ropt.Dialer)
}

func TestNewUniversalClient_ClusterMode(t *testing.T) {
	cnf := &config.Config{Redis: &config.RedisConfig{ClusterMode: true}}
	ropt := common.NewUniversalOptions(cnf, []string{"localhost:6379"}, 0, "")

	client := common.NewUniversalClient(cnf, ropt)
	defer client.Close()

	assert.Contains(t, fmt.Sprintf("%T", client), "Cluster")
}

func TestNewUniversalClient_Default(t *testing.T) {
	cnf := &config.Config{}
	ropt := common.NewUniversalOptions(cnf, []string{"localhost:6379"}, 0, "")

	client := common.NewUniversalClient(cnf, ropt)
	defer client.Close()

	assert.NotContains(t, fmt.Sprintf("%T", client), "Cluster")
}
