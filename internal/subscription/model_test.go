//go:build unit
// +build unit

package subscription

import (
	"testing"

	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/internal/credentials"
	"github.com/stretchr/testify/assert"
)

func getDummySubscriptionModel() *Model {
	return &Model{
		Name:                           "projects/test-project/subscriptions/test-subscription",
		Labels:                         map[string]string{"label": "value"},
		ExtractedSubscriptionProjectID: "test-project",
		ExtractedTopicProjectID:        "test-project",
		ExtractedTopicName:             "test-topic",
		ExtractedSubscriptionName:      "test-subscription",
		Topic:                          "projects/test-project/topics/test-topic",
		DeadLetterPolicy: &DeadLetterPolicy{
			DeadLetterTopic:     "projects/test-project/topics/test-subscription-dlq",
			MaxDeliveryAttempts: 10,
		},
		PushConfig: &PushConfig{
			PushEndpoint: "www.some-url.com",
			Credentials: &credentials.Model{
				Username:  "u",
				Password:  "p",
				ProjectID: "dummy1",
			},
		},
	}
}

func TestModel_Prefix(t *testing.T) {
	dSubscription := getDummySubscriptionModel()
	assert.Equal(t, common.GetBasePrefix()+Prefix+dSubscription.ExtractedSubscriptionProjectID+"/", dSubscription.Prefix())
}

func TestModel_Key(t *testing.T) {
	dSubscription := getDummySubscriptionModel()
	assert.Equal(t, dSubscription.Prefix()+dSubscription.ExtractedSubscriptionName, dSubscription.Key())
}

func Test_Model(t *testing.T) {
	dSubscription := getDummySubscriptionModel()

	assert.True(t, dSubscription.IsPush())
	assert.Equal(t, "projects/test-project/topics/test-topic", dSubscription.GetTopic())
	assert.Equal(t, "projects/test-project/topics/test-subscription-subscription-internal", dSubscription.GetSubscriptionTopic())
	assert.Equal(t, "projects/test-project/topics/test-subscription-retry", dSubscription.GetRetryTopic())
	assert.Equal(t, "projects/test-project/topics/test-subscription-dlq", dSubscription.GetDeadLetterTopic())

	assert.NotNil(t, dSubscription.GetCredentials())
	assert.True(t, dSubscription.HasCredentials())
	dSubscription.PushConfig = nil
	assert.False(t, dSubscription.IsPush())
	assert.Nil(t, dSubscription.GetCredentials())
	assert.False(t, dSubscription.HasCredentials())

	dSubscription.setDefaultRetryPolicy()
	assert.Equal(t, uint(5), dSubscription.RetryPolicy.MinimumBackoff)
	assert.Equal(t, uint(5), dSubscription.RetryPolicy.MaximumBackoff)

	dSubscription.DeadLetterPolicy = nil
	assert.Equal(t, "projects/test-project/topics/test-subscription-dlq", dSubscription.GetDeadLetterTopic())

	dSubscription.setDefaultDeadLetterPolicy()
	assert.Equal(t, int32(5), dSubscription.DeadLetterPolicy.MaxDeliveryAttempts)
	assert.Equal(t, "projects/test-project/topics/test-subscription-dlq", dSubscription.DeadLetterPolicy.DeadLetterTopic)

	delayTopics := dSubscription.GetDelayTopics()
	assert.Equal(t, 8, len(delayTopics))
	assert.Equal(t, 8, len(dSubscription.GetDelayTopicsMap()))
	assert.Equal(t, []string{"projects/test-project/topics/test-subscription.delay.5.seconds", "projects/test-project/topics/test-subscription.delay.30.seconds", "projects/test-project/topics/test-subscription.delay.60.seconds", "projects/test-project/topics/test-subscription.delay.150.seconds", "projects/test-project/topics/test-subscription.delay.300.seconds", "projects/test-project/topics/test-subscription.delay.600.seconds", "projects/test-project/topics/test-subscription.delay.1800.seconds", "projects/test-project/topics/test-subscription.delay.3600.seconds"}, delayTopics)

	assert.Equal(t, "test-subscription.delay.5.seconds-cg", dSubscription.GetDelayConsumerGroupID("test-subscription.delay.5.seconds"))
	assert.Equal(t, "test-subscription.delay.5.seconds-subscriberID", dSubscription.GetDelayConsumerGroupInstanceID("subscriberID", "test-subscription.delay.5.seconds"))
}

func TestModel_GetRedactedPushEndpoint(t *testing.T) {
	tests := []struct {
		url    string
		assert func(assert.TestingT, interface{}, interface{}, ...interface{}) bool
	}{
		{
			url:    "https://google.com/",
			assert: assert.Equal,
		},
		{
			url:    "https://username:password@google.com",
			assert: assert.NotEqual,
		},
	}
	sub := getDummySubscriptionModel()
	for _, test := range tests {
		sub.PushConfig.PushEndpoint = test.url
		test.assert(t, test.url, sub.GetRedactedPushEndpoint())
	}
}
