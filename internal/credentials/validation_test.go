// +build unit

package credentials

import (
	"context"
	"testing"

	"github.com/razorpay/metro/pkg/encryption"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/stretchr/testify/assert"
)

func TestValidation_IsValidUsername1(t *testing.T) {
	assert.True(t, IsValidUsername("perf10__q1w2e3"))
}

func TestValidation_IsValidUsername2(t *testing.T) {
	assert.True(t, IsValidUsername("perf-10__q1w2e3"))
}

func TestValidation_IsValidUsername3(t *testing.T) {
	assert.False(t, IsValidUsername("perf10q1w2e3"))
}

func TestValidation_IsValidUsername4(t *testing.T) {
	assert.False(t, IsValidUsername("perf10__q1w2"))
}

func TestValidation_fromProto(t *testing.T) {
	proto := &metrov1.ProjectCredentials{
		ProjectId: "project007",
	}
	encryption.RegisterEncryptionKey("2K9HQKejNV0OkycszeuZ7e6QKwtbwrzO")
	m, err := GetValidatedModelForCreate(context.Background(), proto)
	assert.Nil(t, err)
	assert.Equal(t, "project007", m.GetProjectID())
	assert.True(t, usernameRegex.MatchString(m.GetUsername()))
	assert.Equal(t, 20, len(m.GetPassword()))
}
