// +build unit

package app

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBoot_GetEnv(t *testing.T) {
	os.Setenv("APP_ENV", "test")
	env := GetEnv()
	assert.Equal(t, "test", env)
	os.Setenv("APP_ENV", "")
	env = GetEnv()
	assert.Equal(t, "dev", env)
}
