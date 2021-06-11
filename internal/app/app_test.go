// +build unit

package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBoot_GetEnv(t *testing.T) {
	Env = "test"
	env := GetEnv()
	assert.Equal(t, "test", env)
	Env = "dev"
	env = GetEnv()
	assert.Equal(t, "dev", env)
}
