//go:build unit
// +build unit

package registry

import (
	"testing"

	"github.com/magiconair/properties/assert"
)

func TestGetKeys(t *testing.T) {
	res := GetKeys([]Pair{{"k1", []byte("v1"), "v1", "s1"}})
	assert.Equal(t, res, []string{"k1"})
}

func TestGetKeysNilPairs(t *testing.T) {
	res := GetKeys(nil)
	assert.Equal(t, res, []string{})
}

func TestGetKeysMultiplePairs(t *testing.T) {
	res := GetKeys([]Pair{{"k1", []byte("v1"), "v1", "s1"}, {"k2", []byte("v2"), "v1", "s2"}})
	assert.Equal(t, res, []string{"k1", "k2"})
}
