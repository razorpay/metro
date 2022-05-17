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
	offset := getDummyOffsetModel()
	ctx := context.Background()

	tests := []struct {
		exists bool
	}{
		{
			exists: true,
		},
		{
			exists: false,
		},
	}

	for _, test := range tests {
		mockRepo.EXPECT().Exists(gomock.Any(), offset.Key()).Return(test.exists, nil)
		mockRepo.EXPECT().Save(gomock.Any(), offset)
		mockRepo.EXPECT().Get(gomock.Any(), offset.Key(), offset).DoAndReturn(
			func(arg1 context.Context, arg2 string, arg3 *Model) *Model {
				arg3.rollbackOffset = 1
				return arg3
			}).AnyTimes()
		err := core.SetOffset(ctx, offset)
		assert.NoError(t, err)
	}
}

func TestCore_SetOffsetStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	oStatus := getDummyOffsetStatusModel()
	ctx := context.Background()
	mockRepo.EXPECT().Save(gomock.Any(), oStatus)
	err := core.SetOffsetStatus(ctx, oStatus)
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

func TestCore_OffsetStatusExists(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	oStatus := getDummyOffsetStatusModel()
	ctx := context.Background()
	mockRepo.EXPECT().Exists(gomock.Any(), oStatus.Key()).Return(true, nil)
	ok, err := core.OffsetStatusExists(ctx, oStatus)
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

func TestCore_GetOffsetStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	oStatus := getDummyOffsetStatusModel()
	ctx := context.Background()
	mockRepo.EXPECT().Get(gomock.Any(), oStatus.Key(), gomock.Any()).Return(nil)
	_, err := core.GetOffsetStatus(ctx, oStatus)
	assert.NoError(t, err)
	assert.NotNil(t, oStatus)
}

func TestCore_DeleteOffset(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	offset := getDummyOffsetModel()
	ctx := context.Background()
	mockRepo.EXPECT().Exists(gomock.Any(), offset.Key()).Return(true, nil)
	mockRepo.EXPECT().Delete(gomock.Any(), offset).Return(nil)
	err := core.DeleteOffset(ctx, offset)
	assert.NoError(t, err)
	assert.NotNil(t, offset)
}

func TestCore_DeleteOffsetStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	oStatus := getDummyOffsetStatusModel()
	ctx := context.Background()
	mockRepo.EXPECT().Exists(gomock.Any(), oStatus.Key()).Return(true, nil)
	mockRepo.EXPECT().Delete(gomock.Any(), oStatus).Return(nil)
	err := core.DeleteOffsetStatus(ctx, oStatus)
	assert.NoError(t, err)
	assert.NotNil(t, oStatus)
}

func TestCore_RollBackOffset(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	offset := getDummyOffsetModel()
	ctx := context.Background()
	mockRepo.EXPECT().Exists(gomock.Any(), offset.Key()).Return(true, nil)
	mockRepo.EXPECT().Get(gomock.Any(), offset.Key(), offset).DoAndReturn(
		func(arg1 context.Context, arg2 string, arg3 *Model) *Model {
			arg3.rollbackOffset = 1
			return arg3
		})
	mockRepo.EXPECT().Save(gomock.Any(), offset)
	err := core.RollBackOffset(ctx, offset)
	assert.NoError(t, err)
}
