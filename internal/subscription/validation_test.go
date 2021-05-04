package subscription

import (
	"context"
	"testing"

	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/stretchr/testify/assert"
)

func Test_extractSubscriptionMetaAndValidate(t *testing.T) {
	ctx := context.Background()
	proj, subs, err := extractSubscriptionMetaAndValidate(ctx, "projects/test-project/subscriptions/test-sub")
	assert.Nil(t, err)
	assert.Equal(t, "test-project", proj)
	assert.Equal(t, "test-sub", subs)

	// should fail as subscription name contains invalid char
	proj, subs, err = extractSubscriptionMetaAndValidate(ctx, "projects/test-project/subscriptions/test-sub$")
	assert.NotNil(t, err)

	// should fail as subscription name has invalid format
	proj, subs, err = extractSubscriptionMetaAndValidate(ctx, "projects/test-project/test-sub")
	assert.NotNil(t, err)

	// should fail as subscription name starts with goog
	proj, subs, err = extractSubscriptionMetaAndValidate(ctx, "projects/test-project/subscriptions/goog-test-sub")
	assert.NotNil(t, err)

	// should fail as subscription name has invalid length
	proj, subs, err = extractSubscriptionMetaAndValidate(ctx, "projects/test-project/subscriptions/to")
	assert.NotNil(t, err)
}

func Test_validateTopicName(t *testing.T) {
	ctx := context.Background()

	name, err := validateTopicName(ctx, "projects/test-project/topics/test-topic")
	assert.Nil(t, err)
	assert.Equal(t, "projects/test-project/topics/test-topic", name)

	// should fail as subscription topic name ends with -retry
	name, err = validateTopicName(ctx, "projects/test-project/topics/test-topic-retry")
	assert.NotNil(t, err)

}

func Test_validatePushConfig(t *testing.T) {
	ctx := context.Background()

	url, err := validatePushConfig(ctx, &metrov1.PushConfig{PushEndpoint: "https://www.razorpay.com"})
	assert.Nil(t, err)
	assert.Equal(t, "https://www.razorpay.com", url)

	// nil push config
	url, err = validatePushConfig(ctx, nil)
	assert.Nil(t, err)
	assert.Equal(t, "", url)

	// nil push config
	url, err = validatePushConfig(ctx, &metrov1.PushConfig{PushEndpoint: "invalid url"})
	assert.NotNil(t, err)
}
