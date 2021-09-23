package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Encode(t *testing.T) {
	s := "dummy"
	assert.Equal(t, "ZHVtbXk=", Encode(s))
}

func Test_Decode(t *testing.T) {
	s := "ZHVtbXk="
	assert.Equal(t, "dummy", Decode(s))
}

func Test_DecodeSlice(t *testing.T) {
	input := []string{"ZHVtbXk=", "c2FtcGxl", "dGVzdA=="}
	expectedOutput := []string{"dummy", "sample", "test"}
	assert.Equal(t, expectedOutput, DecodeSlice(input))
}
