package common

import (
	"testing"

	"github.com/razorpay/metro/internal/app"

	"github.com/stretchr/testify/assert"
)

func TestBaseModel_Prefix(t *testing.T) {
	assert.Equal(t, "metro-"+app.GetEnv()+"/", GetBasePrefix())
}
