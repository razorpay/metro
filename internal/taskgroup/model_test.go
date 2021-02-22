package taskgroup

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func getDummyTaskGroupModel() *Model {
	return &Model{
		ID:           uuid.New().String(),
		Topic:        "projects/test-project/topics/test-topic",
		Broker:       "kafka",
		CurrentCount: 1,
		MaxCount:     1,
		MinCount:     1,
	}
}

func TestModel_Prefix(t *testing.T) {
	dTaskGroup := getDummyTaskGroupModel()
	assert.Equal(t, "metro/taskgroups/", dTaskGroup.Prefix())
}

func TestModel_Key(t *testing.T) {
	dTaskGroup := getDummyTaskGroupModel()
	assert.Equal(t, "metro/taskgroups/"+dTaskGroup.ID, dTaskGroup.Key())
}
