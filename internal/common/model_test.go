package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBaseModel_Prefix(t *testing.T) {
	baseModel := &BaseModel{}
	prefix := baseModel.Prefix()
	assert.Equal(t, "registry/", prefix)
}
