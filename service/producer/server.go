package producer

import (
	"context"
	"log"

	producerv1 "github.com/razorpay/metro/rpc/metro/producer/v1"
)

// Server has methods implementing of server rpc.
type Server struct {
	core *Core
}

func NewServer(core *Core) *Server {
	return &Server{core: core}
}

func (s Server) Produce(ctx context.Context, request *producerv1.ProduceRequest) (*producerv1.ProduceResponse, error) {

	log.Println("produce request received")

	msgIds := make([]string, 0)

	for _, msg := range request.Messages {
		msgId, _ := s.core.Broker.Produce(request.Topic, msg.Data)
		msgIds = append(msgIds, msgId)
	}

	log.Println("produce request completed")

	return &producerv1.ProduceResponse{MessageIds: msgIds}, nil
}
