// +build unit

package registry

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateConsulRegistry(t *testing.T) {
	config := Config{
		Driver:       Consul,
		ConsulConfig: ConsulConfig{},
	}
	reg, err := NewRegistry(&config)
	assert.NotNil(t, reg)
	assert.Nil(t, err)
}

func TestInvalidRegistryDriver(t *testing.T) {
	config := Config{
		Driver: "invalid",
	}
	reg, err := NewRegistry(&config)
	assert.NotNil(t, err)
	assert.Nil(t, reg)
}
