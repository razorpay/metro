package auth

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Auth(t *testing.T) {
	auth := NewAuth("admin", "supersecret")
	assert.Equal(t, "admin", auth.Username)
	assert.Equal(t, "admin", auth.GetUsername())
	assert.Equal(t, "c3VwZXJzZWNyZXQ=", auth.Password)
	assert.Equal(t, "supersecret", auth.GetPassword())
}
