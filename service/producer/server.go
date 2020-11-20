package producer

import (
	"context"

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
	panic("implement me")
}
