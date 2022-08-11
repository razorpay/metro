// +build unit

package messagebroker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_flattenMapSlice(t *testing.T) {
	attributes := make([]map[string][]byte, 1)
	attributes = append(attributes, map[string][]byte{
		"test-attribute": []byte("test-attribute-value"),
	})
	flattenedMap := flattenMapSlice(attributes)
	for _, attribute := range attributes {
		for key, value := range attribute {
			assert.NotNil(t, flattenedMap[key])
			assert.Equal(t, flattenedMap[key], string(value))
		}
	}
}
