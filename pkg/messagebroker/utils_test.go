package messagebroker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_NormalizeTopicName(t *testing.T) {
	assert.Equal(t, "p1_s1", NormalizeTopicName("p1/s1"))
	assert.Equal(t, "p1_s1_t1", NormalizeTopicName("p1/s1/t1"))
}
