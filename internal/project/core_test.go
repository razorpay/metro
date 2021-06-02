// +build unit

package project

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	mocks "github.com/razorpay/metro/internal/project/mocks/repo"
	"github.com/stretchr/testify/assert"
)

func TestProject_NewCore(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	c := NewCore(mockRepo)
	assert.NotNil(t, c)
}

func TestCore_CreateProject(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	project := getDummyProjectModel()
	ctx := context.Background()
	mockRepo.EXPECT().Exists(ctx, project.Key())
	mockRepo.EXPECT().Save(ctx, project)
	err := core.CreateProject(ctx, project)
	assert.NoError(t, err)
}

func TestCore_Exists(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	project := getDummyProjectModel()
	ctx := context.Background()
	mockRepo.EXPECT().Exists(ctx, project.Key()).Return(true, nil)
	ok, err := core.Exists(ctx, project.Key())
	assert.True(t, ok)
	assert.NoError(t, err)
}

func TestCore_ExistsWithID(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	project := getDummyProjectModel()
	ctx := context.Background()
	mockRepo.EXPECT().Exists(ctx, project.Key()).Return(true, nil)
	ok, err := core.ExistsWithID(ctx, project.ProjectID)
	assert.True(t, ok)
	assert.NoError(t, err)
}
