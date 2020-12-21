package producer

import (
	"context"
	"log"

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
func (s publisherServer) Produce(ctx context.Context, request *metrov1.ProduceRequest) (*metrov1.ProduceResponse, error) {

	log.Println("produce request received")

	msgIds := make([]string, 0)

	for _, msg := range request.Messages {
		msgID, _ := s.broker.Produce(request.Topic, msg.Data)
		msgIds = append(msgIds, msgID)
	}

	log.Println("produce request completed")

	return &metrov1.ProduceResponse{MessageIds: msgIds}, nil
}
