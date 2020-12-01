package health

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	healthv1 "github.com/razorpay/metro/rpc/common/health/v1"
)

// Server has methods implementing of server rpc.
type Server struct {
	core *Core
}

func NewServer(core *Core) *Server {
	return &Server{core: core}
}

// Check returns service's serving status.
func (h *Server) Check(_ context.Context, req *healthv1.HealthCheckRequest) (*healthv1.HealthCheckResponse, error) {
	if !h.core.IsHealthy() {
		return nil, status.Error(codes.Unavailable, "Unhealthy")
	}

	return &healthv1.HealthCheckResponse{
		ServingStatus: healthv1.HealthCheckResponse_SERVING_STATUS_SERVING,
	}, nil
}
