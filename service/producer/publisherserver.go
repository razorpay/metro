package producer

import (
	"context"
	"log"

	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/internal/topic"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

type publisherServer struct {
	producer  messagebroker.Producer
	topicCore topic.ICore
}

func newPublisherServer(producer messagebroker.Producer, topicCore topic.ICore) *publisherServer {
	return &publisherServer{producer: producer, topicCore: topicCore}
}

// Produce messages to a topic
func (s publisherServer) Publish(ctx context.Context, req *metrov1.PublishRequest) (*metrov1.PublishResponse, error) {

	log.Println("produce request received")

	msgIds := make([]string, 0)

	for _, msg := range req.Messages {
		msgResp, _ := s.producer.SendMessages(ctx, messagebroker.SendMessageToTopicRequest{
			Topic:   req.Topic,
			Message: msg.Data,
		})
		msgIds = append(msgIds, msgResp.MessageID)
	}

	log.Println("produce request completed")

	return &metrov1.PublishResponse{MessageIds: msgIds}, nil
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
