package interceptors

import (
	"context"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/razorpay/metro/pkg/logger"
	"google.golang.org/grpc"
)

// UnaryServerLoggerInterceptor injects a logger
func UnaryServerLoggerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		t := grpc_ctxtags.Extract(ctx)
		l := logger.Ctx(ctx).With(logger.MapToSliceOfKV(t.Values())...)
		ctx = context.WithValue(ctx, logger.CtxKey, l)
		resp, err := handler(ctx, req)
		if err != nil {
			l.Errorw("error in grpc handler", "info", info, "payload", req, "error", err.Error())
		}
		return resp, err
	}
}
