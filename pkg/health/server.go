package health

import (
	"context"

	healthv1 "github.com/razorpay/metro/rpc/common/health/v1"
	"github.com/twitchtv/twirp"
)

// Server has methods implementing of server rpc.
type Server struct {
	core *Core
}

// NewServer returns a server.
func NewServer(core *Core) *Server {
	return &Server{
		core: core,
	}
}

// Check returns service's serving status.
func (s *Server) Check(ctx context.Context, req *healthv1.HealthCheckRequest) (*healthv1.HealthCheckResponse, error) {
	var status healthv1.HealthCheckResponse_ServingStatus
	ok, err := s.core.RunHealthCheck(ctx)
	if !ok {
		status = healthv1.HealthCheckResponse_SERVING_STATUS_NOT_SERVING
		return &healthv1.HealthCheckResponse{ServingStatus: status}, twirp.NewError(twirp.Unavailable, err.Error())
	}
	status = healthv1.HealthCheckResponse_SERVING_STATUS_SERVING
	return &healthv1.HealthCheckResponse{ServingStatus: status}, nil
}
