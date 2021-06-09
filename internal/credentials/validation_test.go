// +build unit

package credentials

import (
	"context"
	"testing"

	"github.com/razorpay/metro/internal/app"

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
	m, err := GetValidatedModelForCreate(context.Background(), proto)
	assert.Nil(t, err)
	assert.Equal(t, "project007", m.GetProjectID())
	assert.True(t, usernameRegex.MatchString(m.GetUsername()))
	assert.Equal(t, 20, len(m.GetPassword()))
}

func TestValidation_IsAuthorized1(t *testing.T) {

	app.Env = "dev"
	defer func() {
		app.Env = ""
	}()

	ctx := context.WithValue(context.Background(), CtxKey, NewCredential("project007__c525c7", "l0laNoI360l4uvD96682"))
	assert.True(t, IsAuthorized(ctx, "project007")) // matching project
}

func TestValidation_IsAuthorized2(t *testing.T) {

	app.Env = "dev"
	defer func() {
		app.Env = ""
	}()

	ctx := context.WithValue(context.Background(), CtxKey, NewCredential("project007__c525c7", "l0laNoI360l4uvD96682"))
	assert.False(t, IsAuthorized(ctx, "project999")) // project mismatch
}
