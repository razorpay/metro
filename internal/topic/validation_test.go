// +build unit

package topic

import (
	"context"
	"testing"

	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/stretchr/testify/assert"
)

func TestValidation_extractTopicMetaAndValidate(t *testing.T) {
	ctx := context.Background()
	proj, top, err := ExtractTopicMetaAndValidate(ctx, "projects/test-project/topics/test-topic")
	assert.Nil(t, err)
	assert.Equal(t, "test-project", proj)
	assert.Equal(t, "test-topic", top)

	proj, top, err = ExtractTopicMetaAndValidate(ctx, "projects/test-project/topics/test-topic$")
	assert.NotNil(t, err)

	proj, top, err = ExtractTopicMetaAndValidate(ctx, "projects/test-project/test-topic")
	assert.NotNil(t, err)

	proj, top, err = ExtractTopicMetaAndValidate(ctx, "projects/test-project/topics/goog-test-topic")
	assert.NotNil(t, err)

	proj, top, err = ExtractTopicMetaAndValidate(ctx, "projects/test-project/topics/test-topic-retry")
	assert.NotNil(t, err)

	proj, top, err = ExtractTopicMetaAndValidate(ctx, "projects/test-project/topics/test-topic-dlq")
	assert.NotNil(t, err)

	proj, top, err = ExtractTopicMetaAndValidate(ctx, "projects/test-project/topics/to")
	assert.NotNil(t, err)
}

func TestValidation_GetValidatedModel(t *testing.T) {
	ctx := context.Background()

	topic := "projects/test-project/topics/test-topic"
	m, err := GetValidatedModel(ctx, &metrov1.Topic{
		Name:   topic,
		Labels: nil,
	})
	assert.Nil(t, err)

	assert.Equal(t, m.Name, topic)
}
