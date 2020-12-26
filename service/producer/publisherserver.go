package producer

import (
	"context"
	"log"

	"github.com/razorpay/metro/pkg/logger"

	"github.com/razorpay/metro/pkg/messagebroker"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

type publisherServer struct {
	broker messagebroker.Broker
}

func newPublisherServer(broker messagebroker.Broker) *publisherServer {
	return &publisherServer{broker: broker}
}

// Produce messages to a topic
func (s publisherServer) Publish(ctx context.Context, request *metrov1.PublishRequest) (*metrov1.PublishResponse, error) {

	log.Println("produce request received")

	msgIds := make([]string, 0)

	for _, msg := range request.Messages {
		msgID, _ := s.broker.Produce(request.Topic, msg.Data)
		msgIds = append(msgIds, msgID)
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
