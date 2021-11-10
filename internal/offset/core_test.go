package offset

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	mocks "github.com/razorpay/metro/internal/offset/mocks/repo"
	"github.com/stretchr/testify/assert"
)

func TestNewCore(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	c := NewCore(mockRepo)
	assert.NotNil(t, c)
}

func TestCore_SetOffset(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	project := getDummyOffsetModel()
	ctx := context.Background()
	mockRepo.EXPECT().Exists(gomock.Any(), project.Key())
	mockRepo.EXPECT().Save(gomock.Any(), project)
	err := core.SetOffset(ctx, project)
	assert.NoError(t, err)
}

func TestCore_Exists(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	offset := getDummyOffsetModel()
	ctx := context.Background()
	mockRepo.EXPECT().Exists(gomock.Any(), offset.Key()).Return(true, nil)
	ok, err := core.Exists(ctx, offset)
	assert.True(t, ok)
	assert.NoError(t, err)
}

func TestCore_GetOffset(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	offset := getDummyOffsetModel()
	ctx := context.Background()
	mockRepo.EXPECT().Get(gomock.Any(), offset.Key(), gomock.Any()).Return(nil)
	_, err := core.GetOffset(ctx, offset)
	assert.NoError(t, err)
	assert.NotNil(t, offset)
}

func TestCore_DeleteOffset(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	offset := getDummyOffsetModel()
	ctx := context.Background()
	mockRepo.EXPECT().Delete(gomock.Any(), offset).Return(nil)
	err := core.DeleteOffset(ctx, offset)
	assert.NoError(t, err)
	assert.NotNil(t, offset)
}
