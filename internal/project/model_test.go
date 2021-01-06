package project

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestModel_Prefix(t *testing.T) {
	project := getDummyProjectModel()
	assert.Equal(t, project.Prefix(), "registry/projects/")
}

func TestModel_Key(t *testing.T) {
	project := getDummyProjectModel()
	assert.Equal(t, project.Key(), "registry/projects/"+project.ProjectID)
}

func getDummyProjectModel() *Model {
	return &Model{
		Name:      "test",
		ProjectID: "testID",
		Labels:    map[string]string{"label": "value"},
	}
}
