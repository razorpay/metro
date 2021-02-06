package web

import (
	"context"
	"log"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/internal/publisher"
	"github.com/razorpay/metro/internal/topic"
	"github.com/razorpay/metro/pkg/logger"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"google.golang.org/protobuf/types/known/emptypb"
)

type publisherServer struct {
	brokerStore brokerstore.IBrokerStore
	topicCore   topic.ICore
	publisher   publisher.IPublisher
}

func newPublisherServer(brokerStore brokerstore.IBrokerStore, topicCore topic.ICore, publisher publisher.IPublisher) *publisherServer {
	return &publisherServer{brokerStore: brokerStore, topicCore: topicCore, publisher: publisher}
}

// Produce messages to a topic
func (s publisherServer) Publish(ctx context.Context, req *metrov1.PublishRequest) (*metrov1.PublishResponse, error) {
	log.Println("produce request received")
	msgIDs, err := s.publisher.Publish(ctx, req)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}
	log.Println("produce request completed")
	return &metrov1.PublishResponse{MessageIds: msgIDs}, nil
}

// CreateTopic creates a new topic
func (s publisherServer) CreateTopic(ctx context.Context, req *metrov1.Topic) (*metrov1.Topic, error) {
	logger.Ctx(ctx).Infow("received request to create topic", "name", req.Name)
	m, err := topic.GetValidatedModel(ctx, req)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}
	err = s.topicCore.CreateTopic(ctx, m)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}
	return req, nil
}

// Delete a topic
func (s publisherServer) DeleteTopic(ctx context.Context, req *metrov1.DeleteTopicRequest) (*emptypb.Empty, error) {
	logger.Ctx(ctx).Infow("received request to delete topic", "name", req.Topic)
	// Delete topic but not the subscriptions for it
	// the subscriptions would get tagged to _deleted_topic_
	m, err := topic.GetValidatedModel(ctx, &metrov1.Topic{Name: req.Topic})
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}
	err = s.topicCore.DeleteTopic(ctx, m)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}
	return &emptypb.Empty{}, nil
}
