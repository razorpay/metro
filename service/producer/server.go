package producer

import (
	"context"

	"github.com/razorpay/metro/internal/boot"

	producerv1 "github.com/razorpay/metro/rpc/metro/producer/v1"
)

// Server has methods implementing of server rpc.
type Server struct {
	core *Core
}

func NewServer(core *Core) *Server {
	return &Server{core: core}
}

func (h *Server) Produce(ctx context.Context, request *producerv1.ProduceRequest) (*producerv1.ProduceResponse, error) {

	boot.Logger(ctx).Info("produce request received")

	msgIds := make([]string, 0)

	for _, msg := range request.Messages {
		msgId, _ := h.core.PublishMessage(request.Topic, msg.Data)
		msgIds = append(msgIds, msgId)
	}

	boot.Logger(ctx).Info("produce request completed")

	return &producerv1.ProduceResponse{MessageIds: msgIds}, nil
}
