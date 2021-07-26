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

func TestSetVersionID(t *testing.T) {
	m := BaseModel{}
	m.SetVersionID("v1")
	vid, _ := m.GetVersionID()
	assert.Equal(t, "v1", vid)
}

func TestGetVersionIDUnset(t *testing.T) {
	m := BaseModel{}
	_, err := m.GetVersionID()
	assert.NotNil(t, err)
}
