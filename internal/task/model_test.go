package task

import (
	"testing"

	"github.com/google/uuid"

	"github.com/stretchr/testify/assert"
)

func getDummyTaskModel() *Model {
	return &Model{
		ID:               uuid.New().String(),
		TaskGroupID:      uuid.New().String(),
		Broker:           "kafka",
		Topic:            "projects/test-project/topics/test-topic",
		URL:              "https://www.abc.xyz",
		NodeID:           uuid.New().String(),
		Status:           "",
		ErrorDescription: "",
	}
}

func TestModel_Prefix(t *testing.T) {
	dTask := getDummyTaskModel()
	assert.Equal(t, "metro/tasks/", dTask.Prefix())
}

func TestModel_Key(t *testing.T) {
	dTask := getDummyTaskModel()
	assert.Equal(t, "metro/tasks/"+dTask.ID, dTask.Key())
}
