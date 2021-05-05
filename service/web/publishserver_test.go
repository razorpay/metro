package web

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	mocks "github.com/razorpay/metro/internal/brokerstore/mocks"
	mocks3 "github.com/razorpay/metro/internal/publisher/mocks/core"
	mocks2 "github.com/razorpay/metro/internal/topic/mocks/core"
	"github.com/razorpay/metro/pkg/messagebroker"
	mocks4 "github.com/razorpay/metro/pkg/messagebroker/mocks"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"

	"github.com/stretchr/testify/assert"
)

func TestPublishServer_CreateTopicSucess(t *testing.T) {
	ctrl := gomock.NewController(t)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	topicCore := mocks2.NewMockICore(ctrl)
	publisher := mocks3.NewMockIPublisher(ctrl)
	admin := mocks4.NewMockAdmin(ctrl)
	server := newPublisherServer(brokerStore, topicCore, publisher)

	ctx := context.Background()
	req := &metrov1.Topic{
		Name: "projects/project123/topics/test-topic",
	}
	topicCore.EXPECT().CreateTopic(gomock.Any(), gomock.Any()).Times(1).Return(nil)
	brokerStore.EXPECT().GetAdmin(gomock.Any(), gomock.Any()).Times(1).Return(admin, nil)
	admin.EXPECT().CreateTopic(gomock.Any(), gomock.Any()).Times(1).Return(messagebroker.CreateTopicResponse{}, nil)

	topic, err := server.CreateTopic(ctx, req)
	assert.NotNil(t, topic)
	assert.Nil(t, err)
}

func TestPublishServer_CreateTopicFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	topicCore := mocks2.NewMockICore(ctrl)
	publisher := mocks3.NewMockIPublisher(ctrl)
	server := newPublisherServer(brokerStore, topicCore, publisher)

	ctx := context.Background()
	req := &metrov1.Topic{
		Name: "projects/project123/topics/test-topic",
	}
	topicCore.EXPECT().CreateTopic(gomock.Any(), gomock.Any()).Times(1).Return(fmt.Errorf("error"))
	topic, err := server.CreateTopic(ctx, req)
	assert.NotNil(t, err)
	assert.Nil(t, topic)
}

func TestPublishServer_DeleteTopicSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	topicCore := mocks2.NewMockICore(ctrl)
	publisher := mocks3.NewMockIPublisher(ctrl)
	server := newPublisherServer(brokerStore, topicCore, publisher)

	ctx := context.Background()
	req := &metrov1.DeleteTopicRequest{
		Topic: "projects/project123/topics/test-topic",
	}

	topicCore.EXPECT().DeleteTopic(gomock.Any(), gomock.Any()).Times(1).Return(nil)
	_, err := server.DeleteTopic(ctx, req)

	assert.Nil(t, err)
}

func TestPublishServer_DeleteTopicFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	topicCore := mocks2.NewMockICore(ctrl)
	publisher := mocks3.NewMockIPublisher(ctrl)
	server := newPublisherServer(brokerStore, topicCore, publisher)

	ctx := context.Background()
	req := &metrov1.DeleteTopicRequest{
		Topic: "projects/project123/topics/test-topic",
	}

	topicCore.EXPECT().DeleteTopic(gomock.Any(), gomock.Any()).Times(1).Return(fmt.Errorf("error"))
	_, err := server.DeleteTopic(ctx, req)

	assert.NotNil(t, err)
}

func TestPublishServer_PublishSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	topicCore := mocks2.NewMockICore(ctrl)
	publisher := mocks3.NewMockIPublisher(ctrl)
	server := newPublisherServer(brokerStore, topicCore, publisher)

	ctx := context.Background()
	req := &metrov1.PublishRequest{
		Topic:    "projects/project123/topics/test-topic",
		Messages: []*metrov1.PubsubMessage{},
	}

	publisher.EXPECT().Publish(gomock.Any(), gomock.Any()).Times(1).Return([]string{}, nil)
	_, err := server.Publish(ctx, req)
	assert.Nil(t, err)
}

func TestPublishServer_PublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	topicCore := mocks2.NewMockICore(ctrl)
	publisher := mocks3.NewMockIPublisher(ctrl)
	server := newPublisherServer(brokerStore, topicCore, publisher)

	ctx := context.Background()
	req := &metrov1.PublishRequest{
		Topic:    "projects/project123/topics/test-topic",
		Messages: []*metrov1.PubsubMessage{},
	}

	publisher.EXPECT().Publish(gomock.Any(), gomock.Any()).Times(1).Return([]string{}, fmt.Errorf("error"))
	_, err := server.Publish(ctx, req)
	assert.NotNil(t, err)
}
