package common_test

import (
	"testing"

	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/stretchr/testify/assert"
)

func TestNewBackend(t *testing.T) {
	t.Parallel()

	cnf := &config.Config{DefaultQueue: "queue"}
	backend := common.NewBackend(cnf)

	assert.Equal(t, cnf, backend.GetConfig())
}
