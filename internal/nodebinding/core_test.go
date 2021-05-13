// +build unit

package nodebinding

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	mocks "github.com/razorpay/metro/internal/nodebinding/mocks/repo"
	"github.com/stretchr/testify/assert"
)

func TestNode_NewCore(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	c := NewCore(mockRepo)
	assert.NotNil(t, c)
}

func TestCore_CreateNodeBinding(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	nodebinding := getDummyNodeBindingModel()
	ctx := context.Background()
	mockRepo.EXPECT().Exists(ctx, nodebinding.Key())
	mockRepo.EXPECT().Create(ctx, nodebinding)
	err := core.CreateNodeBinding(ctx, nodebinding)
	assert.NoError(t, err)
}

func TestCore_Exists(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	nodebinding := getDummyNodeBindingModel()
	ctx := context.Background()
	mockRepo.EXPECT().Exists(ctx, nodebinding.Key()).Return(true, nil)
	ok, err := core.Exists(ctx, nodebinding.Key())
	assert.True(t, ok)
	assert.NoError(t, err)
}
