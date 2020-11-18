package interceptors

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	RequestIDHeaderKey = "x-rzp-request-id"
	RequestIDTagKey    = "grpc.request.rzp.id"

	RpcMethodKey = "method"
)

func UnaryServerTagInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		t := grpc_ctxtags.Extract(ctx)
		m, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Internal, "could not extract meta from incoming context")
		}

		// Todo: Add additional context information as required
		t.Set(RequestIDTagKey, getRequestId(m))
		t.Set(RpcMethodKey, info.FullMethod)
		return handler(ctx, req)
	}
}

// getRequestId from request metadata. If not set, we'll generate a new one which will be used by logger, tracer etc
func getRequestId(m metadata.MD) string {
	requestIdHeader := m.Get(RequestIDHeaderKey)
	if len(requestIdHeader) >= 1 {
		return requestIdHeader[0]
	}

	return uuid.New().String()
}
