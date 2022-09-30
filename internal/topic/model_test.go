// +build unit

package topic

import (
	"testing"

	"github.com/razorpay/metro/internal/common"
	"github.com/stretchr/testify/assert"
)

func getDummyTopicModel() *Model {
	return &Model{
		Name:               "projects/test-project/topics/test-topic",
		Labels:             map[string]string{"label": "value"},
		ExtractedProjectID: "test-project",
		ExtractedTopicName: "test-topic",
		NumPartitions:      DefaultNumPartitions,
	}
}

func getDLQDummyTopicModel() *Model {
	return &Model{
		Name:               "projects/test-project/topics/test-topic-dlq",
		Labels:             map[string]string{"label": "value"},
		ExtractedProjectID: "test-project",
		ExtractedTopicName: "test-topic-dlq",
		NumPartitions:      DefaultNumPartitions,
	}
}

func TestModel_Prefix(t *testing.T) {
	dTopic := getDummyTopicModel()
	assert.Equal(t, common.GetBasePrefix()+Prefix+dTopic.ExtractedProjectID+"/", dTopic.Prefix())
}

func TestModel_Key(t *testing.T) {
	dTopic := getDummyTopicModel()
	assert.Equal(t, dTopic.Prefix()+dTopic.ExtractedTopicName, dTopic.Key())
}
