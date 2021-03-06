package node

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	mocks "github.com/razorpay/metro/internal/node/mocks/repo"
	"github.com/stretchr/testify/assert"
)

func TestNode_NewCore(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	c := NewCore(mockRepo)
	assert.NotNil(t, c)
}

func TestCore_CreateNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	node := getDummyNodeModel()
	ctx := context.Background()
	mockRepo.EXPECT().Exists(ctx, node.Key())
	mockRepo.EXPECT().Create(ctx, node)
	err := core.CreateNode(ctx, node)
	assert.NoError(t, err)
}

func TestCore_Exists(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	node := getDummyNodeModel()
	ctx := context.Background()
	mockRepo.EXPECT().Exists(ctx, node.Key()).Return(true, nil)
	ok, err := core.Exists(ctx, node.Key())
	assert.True(t, ok)
	assert.NoError(t, err)
}

func TestCore_ExistsWithID(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRepo := mocks.NewMockIRepo(ctrl)
	core := NewCore(mockRepo)
	node := getDummyNodeModel()
	ctx := context.Background()
	mockRepo.EXPECT().Exists(ctx, node.Key()).Return(true, nil)
	ok, err := core.ExistsWithID(ctx, node.ID)
	assert.True(t, ok)
	assert.NoError(t, err)
}
