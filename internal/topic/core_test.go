package topic

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	brokerstoremock "github.com/razorpay/metro/internal/brokerstore/mocks"
	projectcoremock "github.com/razorpay/metro/internal/project/mocks/core"
	topicrepomock "github.com/razorpay/metro/internal/topic/mocks/repo"
	"github.com/razorpay/metro/pkg/messagebroker"
	messagebrokermock "github.com/razorpay/metro/pkg/messagebroker/mocks"
	"github.com/stretchr/testify/assert"
)

func TestCore_CreateTopic(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockTopicRepo := topicrepomock.NewMockIRepo(ctrl)
	mockProjectCore := projectcoremock.NewMockICore(ctrl)
	mockBrokerStore := brokerstoremock.NewMockIBrokerStore(ctrl)
	mockAdmin := messagebrokermock.NewMockBroker(ctrl)
	topicCore := NewCore(mockTopicRepo, mockProjectCore, mockBrokerStore)
	ctx := context.Background()
	dTopic := getDummyTopicModel()
	mockProjectCore.EXPECT().ExistsWithID(ctx, dTopic.ExtractedProjectID).Return(true, nil)
	mockBrokerStore.EXPECT().GetAdmin(ctx, messagebroker.AdminClientOptions{}).Return(mockAdmin, nil)
	mockAdmin.EXPECT().CreateTopic(ctx, messagebroker.CreateTopicRequest{dTopic.Name, 2}).Return(messagebroker.CreateTopicResponse{}, nil)
	mockTopicRepo.EXPECT().Exists(ctx, "metro/topics/test-project/test-topic")
	mockTopicRepo.EXPECT().Create(ctx, dTopic)
	err := topicCore.CreateTopic(ctx, dTopic)
	assert.Nil(t, err)
}
