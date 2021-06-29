package messagebroker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_NewConsumerClient_WithWrongVariant(t *testing.T) {
	ctx := context.Background()
	variant := "wrong-name"
	c, err := NewConsumerClient(ctx, variant, nil, nil)
	assert.Nil(t, c)
	assert.NotNil(t, err)
}

func Test_NewProducerClient_WithWrongVariant(t *testing.T) {
	ctx := context.Background()
	variant := "wrong-name"
	c, err := NewProducerClient(ctx, variant, nil, nil)
	assert.Nil(t, c)
	assert.NotNil(t, err)
}

func Test_NewAdminClient_WithWrongVariant(t *testing.T) {
	ctx := context.Background()
	variant := "wrong-name"
	c, err := NewAdminClient(ctx, variant, nil, nil)
	assert.Nil(t, c)
	assert.NotNil(t, err)
}
