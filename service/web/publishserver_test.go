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
	mocks3 "github.com/razorpay/metro/internal/publisher/mocks/publisher"
	"github.com/razorpay/metro/internal/topic"
	mocks2 "github.com/razorpay/metro/internal/topic/mocks/core"
	"github.com/razorpay/metro/pkg/messagebroker"
	messagebrokermock "github.com/razorpay/metro/pkg/messagebroker/mocks"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/stretchr/testify/assert"
)

// GetDummyTopicModel to export dummy topic model
func GetDummyTopicModel() *topic.Model {
	return &topic.Model{
		Name:               "projects/test-project/topics/test-topic",
		Labels:             map[string]string{"label": "value"},
		ExtractedProjectID: "test-project",
		ExtractedTopicName: "test-topic",
		NumPartitions:      1,
	}
}

func TestPublishServer_CreateTopicSuccess(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	topicCore := mocks2.NewMockICore(ctrl)
	publisher := mocks3.NewMockIPublisher(ctrl)
	mockProjectCore := mocks5.NewMockICore(ctrl)
	mockCredentialsCore := mocks6.NewMockICore(ctrl)
	server := newPublisherServer(mockProjectCore, brokerStore, topicCore, mockCredentialsCore, publisher)

	req := &metrov1.Topic{
		Name:   "projects/project123/topics/test-topic",
		Labels: map[string]string{"foo": "bar"},
	}
	topicModel, err := topic.GetValidatedModel(ctx, req)
	assert.Nil(t, err)

	topicCore.EXPECT().CreateTopic(gomock.Any(), topicModel).Times(1).Return(nil)

	tp, err := server.CreateTopic(ctx, req)
	assert.Equal(t, req, tp)
	assert.Nil(t, err)
}

func TestPublishServer_CreateTopicFailure(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	mockProjectCore := mocks5.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	topicCore := mocks2.NewMockICore(ctrl)
	publisher := mocks3.NewMockIPublisher(ctrl)
	mockCredentialsCore := mocks6.NewMockICore(ctrl)
	server := newPublisherServer(mockProjectCore, brokerStore, topicCore, mockCredentialsCore, publisher)

	req := &metrov1.Topic{
		Name: "projects/project123/topics/test-topic",
	}
	topicModel, err := topic.GetValidatedModel(ctx, req)
	assert.Nil(t, err)

	topicCore.EXPECT().CreateTopic(gomock.Any(), topicModel).Times(1).Return(fmt.Errorf("error"))
	tp, err := server.CreateTopic(ctx, req)
	assert.NotNil(t, err)
	assert.Nil(t, tp)
}

func TestPublishServer_DeleteTopicSuccess(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	mockProjectCore := mocks5.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	topicCore := mocks2.NewMockICore(ctrl)
	publisher := mocks3.NewMockIPublisher(ctrl)
	mockCredentialsCore := mocks6.NewMockICore(ctrl)
	server := newPublisherServer(mockProjectCore, brokerStore, topicCore, mockCredentialsCore, publisher)

	req := &metrov1.DeleteTopicRequest{
		Topic: "projects/project123/topics/test-topic",
	}

	topicModel, err := topic.GetValidatedModel(ctx, &metrov1.Topic{Name: req.Topic})
	assert.Nil(t, err)

	topicCore.EXPECT().DeleteTopic(gomock.Any(), topicModel).Times(1).Return(nil)
	_, err = server.DeleteTopic(ctx, req)
	assert.Nil(t, err)
}

func TestPublishServer_DeleteTopicFailure(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	mockProjectCore := mocks5.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	topicCore := mocks2.NewMockICore(ctrl)
	publisher := mocks3.NewMockIPublisher(ctrl)
	mockCredentialsCore := mocks6.NewMockICore(ctrl)
	server := newPublisherServer(mockProjectCore, brokerStore, topicCore, mockCredentialsCore, publisher)

	req := &metrov1.DeleteTopicRequest{
		Topic: "projects/project123/topics/test-topic",
	}
	topicModel, err := topic.GetValidatedModel(ctx, &metrov1.Topic{Name: req.Topic})
	assert.Nil(t, err)

	topicCore.EXPECT().DeleteTopic(gomock.Any(), topicModel).Times(1).Return(fmt.Errorf("error"))
	_, err = server.DeleteTopic(ctx, req)
	assert.NotNil(t, err)
}

func TestPublishServer_PublishSuccess(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	mockProjectCore := mocks5.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	topicCore := mocks2.NewMockICore(ctrl)
	publisher := mocks3.NewMockIPublisher(ctrl)
	mockCredentialsCore := mocks6.NewMockICore(ctrl)
	server := newPublisherServer(mockProjectCore, brokerStore, topicCore, mockCredentialsCore, publisher)

	mockAdmin := messagebrokermock.NewMockBroker(ctrl)
	dTopic := GetDummyTopicModel()
	mockAdmin.EXPECT().CreateTopic(gomock.Any(), messagebroker.CreateTopicRequest{dTopic.Name, 1}).Return(messagebroker.CreateTopicResponse{}, nil)
	mockAdmin.CreateTopic(ctx, messagebroker.CreateTopicRequest{dTopic.Name, 1})

	req := &metrov1.PublishRequest{
		Topic: "projects/test-project/topics/test-topic",
		Messages: []*metrov1.PubsubMessage{
			{
				Data: []byte("data"),
			},
		},
	}

	topicCore.EXPECT().ExistsWithName(gomock.Any(), req.Topic).Return(true, nil)
	publisher.EXPECT().Publish(gomock.Any(), req).Times(1).Return([]string{}, nil)
	_, err := server.Publish(ctx, req)
	assert.Nil(t, err)
}

func TestPublishServer_PublishFailure(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	mockProjectCore := mocks5.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	topicCore := mocks2.NewMockICore(ctrl)
	publisher := mocks3.NewMockIPublisher(ctrl)
	mockCredentialsCore := mocks6.NewMockICore(ctrl)
	server := newPublisherServer(mockProjectCore, brokerStore, topicCore, mockCredentialsCore, publisher)

	req := &metrov1.PublishRequest{
		Topic: "projects/project123/topics/test-topic",
		Messages: []*metrov1.PubsubMessage{
			{
				Data: []byte("data"),
			},
		},
	}

	topicCore.EXPECT().ExistsWithName(gomock.Any(), req.Topic).Return(true, nil)
	publisher.EXPECT().Publish(gomock.Any(), req).Times(1).Return([]string{}, fmt.Errorf("error"))
	_, err := server.Publish(ctx, req)
	assert.NotNil(t, err)
}

func Test2PublishServer_PublishFailure(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	mockProjectCore := mocks5.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	topicCore := mocks2.NewMockICore(ctrl)
	publisher := mocks3.NewMockIPublisher(ctrl)
	mockCredentialsCore := mocks6.NewMockICore(ctrl)
	server := newPublisherServer(mockProjectCore, brokerStore, topicCore, mockCredentialsCore, publisher)

	req := &metrov1.PublishRequest{
		Topic: "projects/project123/topics/test-topic",
		Messages: []*metrov1.PubsubMessage{
			{
				Data: []byte("data"),
			},
		},
	}

	topicCore.EXPECT().ExistsWithName(gomock.Any(), req.Topic).Return(false, fmt.Errorf("error"))
	_, err := server.Publish(ctx, req)
	assert.NotNil(t, err)
}

func Test3PublishServer_PublishFailure(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	mockProjectCore := mocks5.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	topicCore := mocks2.NewMockICore(ctrl)
	publisher := mocks3.NewMockIPublisher(ctrl)
	mockCredentialsCore := mocks6.NewMockICore(ctrl)
	server := newPublisherServer(mockProjectCore, brokerStore, topicCore, mockCredentialsCore, publisher)

	req := &metrov1.PublishRequest{
		Topic: "projects/project123/topics/test-topic",
		Messages: []*metrov1.PubsubMessage{
			{
				Data: []byte("data"),
			},
		},
	}

	topicCore.EXPECT().ExistsWithName(gomock.Any(), req.Topic).Return(false, nil)
	_, err := server.Publish(ctx, req)
	assert.NotNil(t, err)
}

func TestPublishServer_PublishFailure_OnValidation(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	mockProjectCore := mocks5.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	topicCore := mocks2.NewMockICore(ctrl)
	publisher := mocks3.NewMockIPublisher(ctrl)
	mockCredentialsCore := mocks6.NewMockICore(ctrl)
	server := newPublisherServer(mockProjectCore, brokerStore, topicCore, mockCredentialsCore, publisher)

	req := &metrov1.PublishRequest{
		Topic:    "projects/project123/topics/test-topic",
		Messages: []*metrov1.PubsubMessage{},
	}

	topicCore.EXPECT().ExistsWithName(gomock.Any(), req.Topic).Return(true, nil)
	_, err := server.Publish(ctx, req)
	assert.NotNil(t, err)
}

func TestPublishServer_PublishFailure_OnWrongTopic(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	mockProjectCore := mocks5.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	topicCore := mocks2.NewMockICore(ctrl)
	publisher := mocks3.NewMockIPublisher(ctrl)
	mockCredentialsCore := mocks6.NewMockICore(ctrl)
	server := newPublisherServer(mockProjectCore, brokerStore, topicCore, mockCredentialsCore, publisher)

	req := &metrov1.PublishRequest{
		Topic: "projects/project123/topics/non-existent-topic",
		Messages: []*metrov1.PubsubMessage{
			{
				Data: []byte("data"),
			},
		},
	}

	topicCore.EXPECT().ExistsWithName(gomock.Any(), req.Topic).Return(true, nil)
	publisher.EXPECT().Publish(gomock.Any(), req).Times(1).Return([]string{}, fmt.Errorf("error"))
	_, err := server.Publish(ctx, req)
	assert.NotNil(t, err)
}

func TestPublishServer_ListProjectTopics(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	mockProjectCore := mocks5.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	topicCore := mocks2.NewMockICore(ctrl)
	publisher := mocks3.NewMockIPublisher(ctrl)
	mockCredentialsCore := mocks6.NewMockICore(ctrl)
	server := newPublisherServer(mockProjectCore, brokerStore, topicCore, mockCredentialsCore, publisher)

	req := &metrov1.ListProjectTopicsRequest{
		ProjectId: "project_1",
	}

	topics := []*topic.Model{
		{
			ExtractedTopicName: "topic_1",
		},
		{
			ExtractedTopicName: "topic_2",
		},
		{
			ExtractedTopicName: "topic_3",
		},
		{
			ExtractedTopicName: "topic_4",
		},
		{
			ExtractedTopicName: "topic_5",
		},
	}
	topicCore.EXPECT().List(gomock.Any(), topic.Prefix+req.ProjectId).Return(topics, nil)

	res, err := server.ListProjectTopics(ctx, req)
	assert.Nil(t, err)
	assert.Equal(t, 5, len(res.Topics))
}

func TestPublishServer_ListProjectTopicsFailure(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	mockProjectCore := mocks5.NewMockICore(ctrl)
	brokerStore := mocks.NewMockIBrokerStore(ctrl)
	topicCore := mocks2.NewMockICore(ctrl)
	publisher := mocks3.NewMockIPublisher(ctrl)
	mockCredentialsCore := mocks6.NewMockICore(ctrl)
	server := newPublisherServer(mockProjectCore, brokerStore, topicCore, mockCredentialsCore, publisher)

	req := &metrov1.ListProjectTopicsRequest{
		ProjectId: "project_1",
	}

	topicCore.EXPECT().List(gomock.Any(), topic.Prefix+req.ProjectId).Return(nil, fmt.Errorf("error"))

	res, err := server.ListProjectTopics(ctx, req)
	assert.NotNil(t, err)
	assert.Nil(t, res)
}
