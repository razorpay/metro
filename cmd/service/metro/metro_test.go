package metro

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBoot_isValidComponent(t *testing.T) {
	var res bool

	res = isValidComponent(Web)
	assert.Equal(t, true, res)

	res = isValidComponent(Worker)
	assert.Equal(t, true, res)

	res = isValidComponent(OpenAPIServer)
	assert.Equal(t, true, res)

	res = isValidComponent("invalid")
	assert.Equal(t, false, res)
}
