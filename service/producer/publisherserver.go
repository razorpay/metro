package producer

import (
	"context"
	"log"

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
func (s publisherServer) Produce(ctx context.Context, request *metrov1.ProduceRequest) (*metrov1.ProduceResponse, error) {

	log.Println("produce request received")

	msgIds := make([]string, 0)

	for _, msg := range request.Messages {
		msgResp, _ := s.producer.SendMessage(ctx, messagebroker.SendMessageToTopicRequest{
			Topic:   request.Topic,
			Message: msg.Data,
		})
		msgIds = append(msgIds, msgResp.MessageID)
	}

	log.Println("produce request completed")

	return &metrov1.ProduceResponse{MessageIds: msgIds}, nil
}
