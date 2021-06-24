// +build unit

package registry

import (
	"github.com/magiconair/properties/assert"
	"testing"
)

func TestPairToString(t *testing.T) {
	pair := Pair{
		Key:       "k1",
		Value:     []byte("v1"),
		SessionID: "s1",
	}

	assert.Equal(t, pair.String(), "key: k1, value: v1, sessionID: s1")
}
