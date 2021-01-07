package topic

import (
	"context"
	"testing"

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

	proj, top, err = ExtractTopicMetaAndValidate(ctx, "projects/test-project/topics/to")
	assert.NotNil(t, err)
}
