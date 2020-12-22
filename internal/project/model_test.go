package project

import (
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/stretchr/testify/assert"

	"testing"
)

func TestModel_FromProto(t *testing.T) {
	projectProto := &metrov1.Project{
		Name:      "test-project",
		ProjectId: "test-project-id",
		Labels:    map[string]string{"foo": "bar"},
	}
	projectModel := FromProto(projectProto)
	assert.Equal(t, projectProto.Name, projectModel.Name)
	assert.Equal(t, projectProto.ProjectId, projectModel.ProjectID)
	assert.Equal(t, projectProto.Labels, projectModel.Labels)

}

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
