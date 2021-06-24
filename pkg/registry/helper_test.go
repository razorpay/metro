// +build unit

package registry

import (
	"github.com/magiconair/properties/assert"
	"testing"
)

func TestGetKeys(t *testing.T) {
	res := GetKeys([]Pair{{"k1", []byte("v1"), "s1"}})
	assert.Equal(t, res, []string{"k1"})
}

func TestGetKeysNilPairs(t *testing.T) {
	res := GetKeys(nil)
	assert.Equal(t, res, []string{})
}

func TestGetKeysMultiplePairs(t *testing.T) {
	res := GetKeys([]Pair{{"k1", []byte("v1"), "s1"}, {"k2", []byte("v2"), "s2"}})
	assert.Equal(t, res, []string{"k1", "k2"})
}
