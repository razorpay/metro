package interceptors

import (
	"context"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/razorpay/metro/pkg/logger"
	"google.golang.org/grpc"

	"github.com/razorpay/metro/internal/boot"
)

func UnaryServerLoggerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		t := grpc_ctxtags.Extract(ctx)
		ctx = context.WithValue(ctx, logger.LoggerCtxKey, boot.Logger(ctx).WithFields(t.Values()))

		resp, err := handler(ctx, req)
		if err != nil {
			boot.Logger(ctx).WithError(err).Error(err.Error())
		}

		return resp, err
	}
}
