package producer

import (
	"context"
	"log"

	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

type publisherServer struct {
	producer messagebroker.Producer
}

func newPublisherServer(producer messagebroker.Producer) *publisherServer {
	return &publisherServer{producer: producer}
}

// Produce messages to a topic
func (s publisherServer) Publish(ctx context.Context, request *metrov1.PublishRequest) (*metrov1.PublishResponse, error) {

	log.Println("produce request received")

	msgIds := make([]string, 0)

	for _, msg := range request.Messages {
		msgResp, _ := s.producer.SendMessages(ctx, messagebroker.SendMessageToTopicRequest{
			Topic:   request.Topic,
			Message: msg.Data,
		})
		msgIds = append(msgIds, msgResp.MessageID)
	}

	log.Println("produce request completed")

	return &metrov1.PublishResponse{MessageIds: msgIds}, nil
}

// CreateTopic creates a new topic
func (s publisherServer) CreateTopic(ctx context.Context, request *metrov1.Topic) (*metrov1.Topic, error) {
	logger.Ctx(ctx).Infow("received request to create topic", "name", request.Name)
	// TODO: Implement
	return request, nil
}
