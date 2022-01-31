// +build unit

package topic

import (
	"context"
	"testing"

	"github.com/razorpay/metro/internal/common"

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
	mockProjectCore.EXPECT().ExistsWithID(gomock.Any(), dTopic.ExtractedProjectID).Return(true, nil)
	mockBrokerStore.EXPECT().GetAdmin(gomock.Any(), messagebroker.AdminClientOptions{}).Return(mockAdmin, nil)
	mockAdmin.EXPECT().CreateTopic(gomock.Any(), messagebroker.CreateTopicRequest{dTopic.Name, DefaultNumPartitions}).Return(messagebroker.CreateTopicResponse{}, nil)
	mockTopicRepo.EXPECT().Exists(gomock.Any(), common.GetBasePrefix()+"topics/test-project/test-topic")
	mockTopicRepo.EXPECT().Save(gomock.Any(), dTopic)
	err := topicCore.CreateTopic(ctx, dTopic)
	assert.Nil(t, err)
}

func TestCore_List(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockTopicRepo := topicrepomock.NewMockIRepo(ctrl)
	mockProjectCore := projectcoremock.NewMockICore(ctrl)
	mockBrokerStore := brokerstoremock.NewMockIBrokerStore(ctrl)
	topicCore := NewCore(mockTopicRepo, mockProjectCore, mockBrokerStore)
	ctx := context.Background()

	topics := []common.IModel{
		&Model{
			Name: "project_1/topic_1",
		},
		&Model{
			Name: "project_1/topic_2",
		},
	}

	prefix := "subscriptions/project_1"
	mockTopicRepo.EXPECT().List(ctx, common.GetBasePrefix()+prefix).Return(topics, nil)
	out, err := topicCore.List(ctx, prefix)

	expectedOutput := []*Model{
		{
			Name: "project_1/topic_1",
		},
		{
			Name: "project_1/topic_2",
		},
	}

	assert.Nil(t, err)
	assert.Equal(t, expectedOutput, out)
}
