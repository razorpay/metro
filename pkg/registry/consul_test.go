package registry

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateConsulClient(t *testing.T) {
	config := ConsulConfig{}
	c1, err := NewConsulClient(context.Background(), &config)
	t.Log(err)
	assert.NotNil(t, c1)
	assert.Nil(t, err)
}
