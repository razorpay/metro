// +build unit

package web

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	mocks "github.com/razorpay/metro/internal/brokerstore/mocks"
	mocks6 "github.com/razorpay/metro/internal/credentials/mocks/core"
	mocks5 "github.com/razorpay/metro/internal/project/mocks/core"
	mocks3 "github.com/razorpay/metro/internal/publisher/mocks/core"
	"github.com/razorpay/metro/internal/topic"
	mocks2 "github.com/razorpay/metro/internal/topic/mocks/core"
	"github.com/razorpay/metro/pkg/messagebroker"
	mocks4 "github.com/razorpay/metro/pkg/messagebroker/mocks"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/stretchr/testify/assert"
)

func TestPublishServer_CreateTopicSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	topicCore := mocks2.NewMockICore(ctrl)
	publisher := mocks3.NewMockIPublisher(ctrl)
	admin := mocks4.NewMockAdmin(ctrl)
	mockProjectCore := mocks5.NewMockICore(ctrl)
	mockCredentialsCore := mocks6.NewMockICore(ctrl)
	server := newPublisherServer(mockProjectCore, brokerStore, topicCore, mockCredentialsCore, publisher)

	ctx := context.Background()
	req := &metrov1.Topic{
		Name:   "projects/project123/topics/test-topic",
		Labels: map[string]string{"foo": "bar"},
	}
	topicModel, err := topic.GetValidatedModel(ctx, req)
	assert.Nil(t, err)

	topicCore.EXPECT().CreateTopic(ctx, topicModel).Times(1).Return(nil)
	brokerStore.EXPECT().GetAdmin(ctx, gomock.Any()).Times(1).Return(admin, nil)
	admin.EXPECT().CreateTopic(ctx, gomock.Any()).Times(1).Return(messagebroker.CreateTopicResponse{}, nil)

	tp, err := server.CreateTopic(ctx, req)
	assert.Equal(t, req, tp)
	assert.Nil(t, err)
}

func TestPublishServer_CreateTopicFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks5.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	topicCore := mocks2.NewMockICore(ctrl)
	publisher := mocks3.NewMockIPublisher(ctrl)
	mockCredentialsCore := mocks6.NewMockICore(ctrl)
	server := newPublisherServer(mockProjectCore, brokerStore, topicCore, mockCredentialsCore, publisher)

	ctx := context.Background()
	req := &metrov1.Topic{
		Name: "projects/project123/topics/test-topic",
	}
	topicModel, err := topic.GetValidatedModel(ctx, req)
	assert.Nil(t, err)

	topicCore.EXPECT().CreateTopic(ctx, topicModel).Times(1).Return(fmt.Errorf("error"))
	tp, err := server.CreateTopic(ctx, req)
	assert.NotNil(t, err)
	assert.Nil(t, tp)
}

func TestPublishServer_DeleteTopicSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks5.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	topicCore := mocks2.NewMockICore(ctrl)
	publisher := mocks3.NewMockIPublisher(ctrl)
	mockCredentialsCore := mocks6.NewMockICore(ctrl)
	server := newPublisherServer(mockProjectCore, brokerStore, topicCore, mockCredentialsCore, publisher)

	ctx := context.Background()
	req := &metrov1.DeleteTopicRequest{
		Topic: "projects/project123/topics/test-topic",
	}

	topicModel, err := topic.GetValidatedModel(ctx, &metrov1.Topic{Name: req.Topic})
	assert.Nil(t, err)

	topicCore.EXPECT().DeleteTopic(ctx, topicModel).Times(1).Return(nil)
	_, err = server.DeleteTopic(ctx, req)
	assert.Nil(t, err)
}

func TestPublishServer_DeleteTopicFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks5.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	topicCore := mocks2.NewMockICore(ctrl)
	publisher := mocks3.NewMockIPublisher(ctrl)
	mockCredentialsCore := mocks6.NewMockICore(ctrl)
	server := newPublisherServer(mockProjectCore, brokerStore, topicCore, mockCredentialsCore, publisher)

	ctx := context.Background()
	req := &metrov1.DeleteTopicRequest{
		Topic: "projects/project123/topics/test-topic",
	}
	topicModel, err := topic.GetValidatedModel(ctx, &metrov1.Topic{Name: req.Topic})
	assert.Nil(t, err)

	topicCore.EXPECT().DeleteTopic(ctx, topicModel).Times(1).Return(fmt.Errorf("error"))
	_, err = server.DeleteTopic(ctx, req)
	assert.NotNil(t, err)
}

func TestPublishServer_PublishSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks5.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	topicCore := mocks2.NewMockICore(ctrl)
	publisher := mocks3.NewMockIPublisher(ctrl)
	mockCredentialsCore := mocks6.NewMockICore(ctrl)
	server := newPublisherServer(mockProjectCore, brokerStore, topicCore, mockCredentialsCore, publisher)

	ctx := context.Background()
	req := &metrov1.PublishRequest{
		Topic:    "projects/project123/topics/test-topic",
		Messages: []*metrov1.PubsubMessage{},
	}

	topicCore.EXPECT().ExistsWithName(ctx, req.Topic).Return(true, nil)
	publisher.EXPECT().Publish(ctx, req).Times(1).Return([]string{}, nil)
	_, err := server.Publish(ctx, req)
	assert.Nil(t, err)
}

func TestPublishServer_PublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks5.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	topicCore := mocks2.NewMockICore(ctrl)
	publisher := mocks3.NewMockIPublisher(ctrl)
	mockCredentialsCore := mocks6.NewMockICore(ctrl)
	server := newPublisherServer(mockProjectCore, brokerStore, topicCore, mockCredentialsCore, publisher)

	ctx := context.Background()
	req := &metrov1.PublishRequest{
		Topic:    "projects/project123/topics/test-topic",
		Messages: []*metrov1.PubsubMessage{},
	}

	topicCore.EXPECT().ExistsWithName(ctx, req.Topic).Return(true, nil)
	publisher.EXPECT().Publish(ctx, req).Times(1).Return([]string{}, fmt.Errorf("error"))
	_, err := server.Publish(ctx, req)
	assert.NotNil(t, err)
}

func TestPublishServer_PublishFailure_OnWrongTopic(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockProjectCore := mocks5.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	topicCore := mocks2.NewMockICore(ctrl)
	publisher := mocks3.NewMockIPublisher(ctrl)
	mockCredentialsCore := mocks6.NewMockICore(ctrl)
	server := newPublisherServer(mockProjectCore, brokerStore, topicCore, mockCredentialsCore, publisher)

	ctx := context.Background()
	req := &metrov1.PublishRequest{
		Topic:    "projects/project123/topics/non-existent-topic",
		Messages: []*metrov1.PubsubMessage{},
	}

	topicCore.EXPECT().ExistsWithName(ctx, req.Topic).Return(false, nil)
	_, err := server.Publish(ctx, req)
	assert.NotNil(t, err)
}
