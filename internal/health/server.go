package health

import (
	"context"

	"github.com/razorpay/metro/pkg/logger"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server has methods implementing of server rpc.
type Server struct {
	core *Core
}

// NewServer returns a health server
func NewServer(core *Core) *Server {
	return &Server{core: core}
}

// Check returns service's serving status.
func (h *Server) Check(ctx context.Context, req *metrov1.HealthCheckRequest) (*metrov1.HealthCheckResponse, error) {
	if !h.core.IsHealthy(ctx) {
		logger.Ctx(ctx).Debugw("metro health check", "status", "Unhealthy")
		return nil, status.Error(codes.Unavailable, "Unhealthy")
	}

	logger.Ctx(ctx).Debugw("metro health check", "status", "Healthy")
	return &metrov1.HealthCheckResponse{
		ServingStatus: metrov1.HealthCheckResponse_SERVING_STATUS_SERVING,
	}, nil
}
