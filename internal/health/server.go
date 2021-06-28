package health

import (
	"context"

	"github.com/razorpay/metro/pkg/logger"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Server has methods implementing of server rpc.
type Server struct {
	core ICore
}

// ReadinessCheck checks only for the responsiveness of the service
func (h *Server) ReadinessCheck(ctx context.Context, _ *emptypb.Empty) (*metrov1.StatusCheckResponse, error) {
	if !h.core.IsHealthy(ctx) {
		logger.Ctx(ctx).Debugw("metro health check", "status", "Unhealthy")
		return nil, status.Error(codes.Unavailable, "Unhealthy")
	}

	logger.Ctx(ctx).Debugw("metro readiness check", "status", "Healthy")
	return &metrov1.StatusCheckResponse{
		ServingStatus: metrov1.StatusCheckResponse_SERVING_STATUS_SERVING,
	}, nil
}

// LivenessCheck checks responsiveness of all the dependant resources that the service is using
func (h *Server) LivenessCheck(ctx context.Context, _ *emptypb.Empty) (*metrov1.StatusCheckResponse, error) {
	logger.Ctx(ctx).Debugw("metro liveness check", "status", "Healthy")
	return &metrov1.StatusCheckResponse{
		ServingStatus: metrov1.StatusCheckResponse_SERVING_STATUS_SERVING,
	}, nil
}

// NewServer returns a health server
func NewServer(core ICore) *Server {
	return &Server{core: core}
}

// Check returns service's serving status.
func (h *Server) Check(ctx context.Context, req *metrov1.StatusCheckResponse) (*metrov1.StatusCheckResponse, error) {
	if !h.core.IsHealthy(ctx) {
		logger.Ctx(ctx).Debugw("metro health check", "status", "Unhealthy")
		return nil, status.Error(codes.Unavailable, "Unhealthy")
	}

	logger.Ctx(ctx).Debugw("metro health check", "status", "Healthy")
	return &metrov1.StatusCheckResponse{
		ServingStatus: metrov1.StatusCheckResponse_SERVING_STATUS_SERVING,
	}, nil
}
