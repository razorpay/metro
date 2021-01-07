package subscription

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_extractSubscriptionMetaAndValidate(t *testing.T) {
	ctx := context.Background()
	proj, subs, err := extractSubscriptionMetaAndValidate(ctx, "projects/test-project/subscriptions/test-sub")
	assert.Nil(t, err)
	assert.Equal(t, "test-project", proj)
	assert.Equal(t, "test-sub", subs)

	proj, subs, err = extractSubscriptionMetaAndValidate(ctx, "projects/test-project/subscriptions/test-sub$")
	assert.NotNil(t, err)

	proj, subs, err = extractSubscriptionMetaAndValidate(ctx, "projects/test-project/test-sub")
	assert.NotNil(t, err)

	proj, subs, err = extractSubscriptionMetaAndValidate(ctx, "projects/test-project/topics/goog-test-sub")
	assert.NotNil(t, err)

	proj, subs, err = extractSubscriptionMetaAndValidate(ctx, "projects/test-project/subscriptions/to")
	assert.NotNil(t, err)
}
