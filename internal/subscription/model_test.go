package subscription

import (
	"testing"

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
	dTopic := getDummySubscriptionModel()
	assert.Equal(t, "metro/projects/test-project/subscriptions/", dTopic.Prefix())
}

func TestModel_Key(t *testing.T) {
	dTopic := getDummySubscriptionModel()
	assert.Equal(t, "metro/projects/"+dTopic.ExtractedSubscriptionProjectID+"/subscriptions/"+dTopic.ExtractedSubscriptionName, dTopic.Key())
}
