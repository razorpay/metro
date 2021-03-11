package topic

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func getDummyTopicModel() *Model {
	return &Model{
		Name:               "projects/test-project/topics/test-topic",
		Labels:             map[string]string{"label": "value"},
		ExtractedProjectID: "test-project",
		ExtractedTopicName: "test-topic",
	}
}

func TestModel_Prefix(t *testing.T) {
	dTopic := getDummyTopicModel()
	assert.Equal(t, "metro/topics/test-project/", dTopic.Prefix())
}

func TestModel_Key(t *testing.T) {
	dTopic := getDummyTopicModel()
	assert.Equal(t, "metro/topics/"+dTopic.ExtractedProjectID+"/"+dTopic.ExtractedTopicName, dTopic.Key())
}
