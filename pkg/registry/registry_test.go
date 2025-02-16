//go:build unit
// +build unit

package registry

import (
	"testing"

	"github.com/magiconair/properties/assert"
)

func TestPairToString(t *testing.T) {
	pair := Pair{
		Key:       "k1",
		Value:     []byte("v1"),
		SessionID: "s1",
		Version:   "v1",
	}

	assert.Equal(t, pair.String(), "key: k1, value: v1, sessionID: s1, version: v1")
}
