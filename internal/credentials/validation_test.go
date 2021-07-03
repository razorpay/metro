// +build unit

package credentials

import (
	"context"
	"os"
	"testing"

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

func TestValidation_fromProto_Success(t *testing.T) {
	proto := &metrov1.ProjectCredentials{
		ProjectId: "project007",
	}
	m, err := GetValidatedModelForCreate(context.Background(), proto)
	assert.Nil(t, err)
	assert.Equal(t, "project007", m.GetProjectID())
	assert.True(t, usernameRegex.MatchString(m.GetUsername()))
	assert.Equal(t, 20, len(m.GetPassword()))
}

func TestValidation_fromProto_Failure1(t *testing.T) {
	proto := &metrov1.ProjectCredentials{
		ProjectId: "", // empty project
	}
	m, err := GetValidatedModelForCreate(context.Background(), proto)
	assert.Nil(t, m)
	assert.NotNil(t, err)
}

func TestValidation_fromProto_Failure2(t *testing.T) {
	proto := &metrov1.ProjectCredentials{
		ProjectId: "project_!@!@!@$scsidcmosicm_wrong_name", // invalid project
	}
	m, err := GetValidatedModelForCreate(context.Background(), proto)
	assert.Nil(t, m)
	assert.NotNil(t, err)
}

func TestValidation_IsAuthorized1(t *testing.T) {

	currEnv := os.Getenv("APP_ENV")
	os.Setenv("APP_ENV", "non-dev")
	defer func() {
		os.Setenv("APP_ENV", currEnv)
	}()

	creds := NewCredential("project007__c525c7", "l0laNoI360l4uvD96682")
	ctx := context.WithValue(context.Background(), CtxKey.String(), creds)
	assert.True(t, IsAuthorized(ctx, "project007")) // matching project
	assert.False(t, creds.IsAdminType())
}

func TestValidation_IsAuthorized2(t *testing.T) {

	currEnv := os.Getenv("APP_ENV")
	os.Setenv("APP_ENV", "non-dev")
	defer func() {
		os.Setenv("APP_ENV", currEnv)
	}()

	creds := NewCredential("project007__c525c7", "l0laNoI360l4uvD96682")
	ctx := context.WithValue(context.Background(), CtxKey.String(), creds)
	assert.False(t, IsAuthorized(ctx, "project999")) // project mismatch
	assert.False(t, creds.IsAdminType())
}

func TestValidation_IsAuthorized3(t *testing.T) {

	currEnv := os.Getenv("APP_ENV")
	os.Setenv("APP_ENV", "non-dev")
	defer func() {
		os.Setenv("APP_ENV", currEnv)
	}()

	adminCreds := NewAdminCredential("admin", "supersecret")
	assert.True(t, adminCreds.IsAdminType())

	ctx := context.WithValue(context.Background(), CtxKey.String(), adminCreds)
	assert.True(t, IsAuthorized(ctx, "this-should-not-matter-for-admin"))
}

func TestValidation_IsAuthorized4(t *testing.T) {
	currEnv := os.Getenv("APP_ENV")
	os.Setenv("APP_ENV", "non-dev")
	defer func() {
		os.Setenv("APP_ENV", currEnv)
	}()

	// do not set any creds in ctx
	assert.False(t, IsAuthorized(context.Background(), "project1"))
}

func TestValidation_IsAuthorized5(t *testing.T) {
	// test mode validations should be true
	assert.True(t, IsAuthorized(context.Background(), "project1"))
}
