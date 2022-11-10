//go:build unit
// +build unit

package common

import (
	"testing"

	"github.com/razorpay/metro/internal/app"

	"github.com/stretchr/testify/assert"
)

func TestBaseModel_Prefix(t *testing.T) {
	assert.Equal(t, "metro-"+app.GetEnv()+"/", GetBasePrefix())
}

func TestSetVersion(t *testing.T) {
	m := BaseModel{}
	m.SetVersion("v1")
	ver := m.GetVersion()
	assert.Equal(t, "v1", ver)
}

func TestGetVersionUnset(t *testing.T) {
	m := BaseModel{}
	ver := m.GetVersion()
	assert.Equal(t, ver, "0")
}
