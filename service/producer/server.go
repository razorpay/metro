package producer

import (
	"context"
	"log"

	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

type server struct {
	core *core
}

func newServer(core *core) *server {
	return &server{core: core}
}

// Produce messages to a topic
func (s server) Produce(ctx context.Context, request *metrov1.ProduceRequest) (*metrov1.ProduceResponse, error) {

	log.Println("produce request received")

	msgIds := make([]string, 0)

	for _, msg := range request.Messages {
		msgID, _ := s.core.Broker.Produce(request.Topic, msg.Data)
		msgIds = append(msgIds, msgID)
	}

	log.Println("produce request completed")

	return &metrov1.ProduceResponse{MessageIds: msgIds}, nil
}
