package topic

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	projectcoremock "github.com/razorpay/metro/internal/project/mocks/core"
	topicrepomock "github.com/razorpay/metro/internal/topic/mocks/repo"
	"github.com/stretchr/testify/assert"
)

func TestCore_CreateTopic(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockTopicRepo := topicrepomock.NewMockIRepo(ctrl)
	mockProjectCore := projectcoremock.NewMockICore(ctrl)
	topicCore := NewCore(mockTopicRepo, mockProjectCore)
	ctx := context.Background()
	dTopic := getDummyTopicModel()
	mockProjectCore.EXPECT().ExistsWithID(ctx, dTopic.ExtractedProjectID).Return(true, nil)
	mockTopicRepo.EXPECT().Exists(ctx, "metro/projects/test-project/topics/test-topic")
	mockTopicRepo.EXPECT().Create(ctx, dTopic)
	err := topicCore.CreateTopic(ctx, dTopic)
	assert.Nil(t, err)
}
