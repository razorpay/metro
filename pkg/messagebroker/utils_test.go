// +build unit

package messagebroker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_NormalizeTopicName(t *testing.T) {
	assert.Equal(t, "p1_s1", normalizeTopicName("p1/s1"))
	assert.Equal(t, "p1_s1_t1", normalizeTopicName("p1/s1/t1"))
}
