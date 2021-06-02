package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Encode(t *testing.T) {
	s := "dummy"
	assert.Equal(t, "ZHVtbXk=", Encode(s))
}

func TestBaseRepo_DeleteTreecode(t *testing.T) {
	s := "ZHVtbXk="
	assert.Equal(t, "dummy", Decode(s))
}
