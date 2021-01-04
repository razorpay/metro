package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBaseModel_Prefix(t *testing.T) {
	assert.Equal(t, "registry/", BasePrefix)
}
