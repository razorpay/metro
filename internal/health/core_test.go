package health

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHealth_MarkUnhealthy(t *testing.T) {
	c, err := NewCore()
	assert.Nil(t, err)
	assert.Equal(t, true, c.IsHealthy())
	c.MarkUnhealthy()
	assert.Equal(t, false, c.IsHealthy())
}
