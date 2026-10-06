package config_test

import (
	"bufio"
	"os"
	"strings"
	"testing"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/stretchr/testify/assert"
)

func TestNewFromEnvironment(t *testing.T) {
	t.Parallel()

	file, err := os.Open("test.env")
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(file)
	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), "=")
		if len(parts) != 2 {
			continue
		}
		os.Setenv(parts[0], parts[1])
	}

	cnf, err := config.NewFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "broker", cnf.Broker)
	assert.Equal(t, "default_queue", cnf.DefaultQueue)
	assert.Equal(t, "result_backend", cnf.ResultBackend)
	assert.Equal(t, 123456, cnf.ResultsExpireIn)
}

func TestNewFromEnvironment_Error(t *testing.T) {
	t.Setenv("RESULTS_EXPIRE_IN", "not-a-number")

	_, err := config.NewFromEnvironment()
	assert.Error(t, err)
}
