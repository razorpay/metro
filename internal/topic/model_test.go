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

func getDelayTopicModel() *Model {
	return &Model{
		Name:               "projects/test-project/topics/test-topic-delay.5.seconds",
		Labels:             map[string]string{"label": "value"},
		ExtractedProjectID: "test-project",
		ExtractedTopicName: "test-topic-delay.5.seconds",
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

func TestModel_IsPrimaryTopic(t *testing.T) {
	tests := []struct {
		name       string
		topicModel *Model
		expected   bool
	}{
		{
			name:       "Primary topic model",
			topicModel: getDummyTopicModel(),
			expected:   true,
		},
		{
			name:       "DLQ topic model",
			topicModel: getDLQDummyTopicModel(),
			expected:   false,
		},
		{
			name:       "Delay topic model",
			topicModel: getDelayTopicModel(),
			expected:   false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, test.topicModel.IsPrimaryTopic())
		})
	}
}
