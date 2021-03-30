package project

import (
	"testing"

	"github.com/razorpay/metro/internal/common"
	"github.com/stretchr/testify/assert"
)

func TestModel_Prefix(t *testing.T) {
	project := getDummyProjectModel()
	assert.Equal(t, project.Prefix(), common.GetBasePrefix()+"projects/")
}

func TestModel_Key(t *testing.T) {
	project := getDummyProjectModel()
	assert.Equal(t, project.Key(), project.Prefix()+project.ProjectID)
}

func getDummyProjectModel() *Model {
	return &Model{
		Name:      "test",
		ProjectID: "testID",
		Labels:    map[string]string{"label": "value"},
	}
}
