package subscription

import (
	"testing"

	"github.com/razorpay/metro/internal/common"

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
