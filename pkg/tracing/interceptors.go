package tracing

import (
	"context"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
	"github.com/razorpay/metro/pkg/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// Component eg: GRPC
	Component = "component"
	// GRPCStatusCode response status
	GRPCStatusCode = "status.code"
)

var (
	grpcTag = opentracing.Tag{Key: Component, Value: "gRPC"}
)

// UnaryServerInterceptor returns a new unary server interceptor for OpenTracing.
func UnaryServerInterceptor(opts ...Option) grpc.UnaryServerInterceptor {
	o := evaluateOptions(opts)
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if o.filterOutFunc != nil && !o.filterOutFunc(ctx, info.FullMethod) {
			return handler(ctx, req)
		}
		opName := info.FullMethod
		if o.opNameFunc != nil {
			opName = o.opNameFunc(info.FullMethod)
		}
		newCtx, serverSpan := newServerSpanFromInbound(ctx, o.tracer, o.traceHeaderName, opName)
		if o.unaryRequestHandlerFunc != nil {
			o.unaryRequestHandlerFunc(serverSpan, req)
		}
		resp, err := handler(newCtx, req)
		finishServerSpan(ctx, serverSpan, err)
		return resp, err
	}
}

// StreamServerInterceptor returns a new streaming server interceptor for OpenTracing.
func StreamServerInterceptor(opts ...Option) grpc.StreamServerInterceptor {
	o := evaluateOptions(opts)
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if o.filterOutFunc != nil && !o.filterOutFunc(stream.Context(), info.FullMethod) {
			return handler(srv, stream)
		}
		opName := info.FullMethod
		if o.opNameFunc != nil {
			opName = o.opNameFunc(info.FullMethod)
		}
		newCtx, serverSpan := newServerSpanFromInbound(stream.Context(), o.tracer, o.traceHeaderName, opName)
		wrappedStream := grpc_middleware.WrapServerStream(stream)
		wrappedStream.WrappedContext = newCtx
		err := handler(srv, wrappedStream)
		finishServerSpan(newCtx, serverSpan, err)
		return err
	}
}

func newServerSpanFromInbound(ctx context.Context, tracer opentracing.Tracer, traceHeaderName, opName string) (context.Context, opentracing.Span) {
	md := metautils.ExtractIncoming(ctx)
	parentSpanContext, err := tracer.Extract(opentracing.HTTPHeaders, metadataTextMap(md))
	if err != nil && err != opentracing.ErrSpanContextNotFound {
		logger.Ctx(ctx).Infow("grpc_opentracing: failed parsing trace information: %v", err)
	}

	serverSpan := tracer.StartSpan(
		opName,
		// this is magical, it attaches the new span to the parent parentSpanContext, and creates an unparented one if empty.
		ext.RPCServerOption(parentSpanContext),
		grpcTag,
	)

	injectOpentracingIdsToTags(traceHeaderName, serverSpan, grpc_ctxtags.Extract(ctx))
	return opentracing.ContextWithSpan(ctx, serverSpan), serverSpan
}

func finishServerSpan(ctx context.Context, serverSpan opentracing.Span, err error) {
	// Log context information
	tags := grpc_ctxtags.Extract(ctx)
	for k, v := range tags.Values() {
		// Don't tag errors, log them instead.
		if vErr, ok := v.(error); ok {
			serverSpan.LogKV(k, vErr.Error())
		} else {
			serverSpan.SetTag(k, v)
		}
	}

	if err != nil {
		if s, ok := status.FromError(err); ok {
			serverSpan.SetTag(GRPCStatusCode, s.Code())
		}

		ext.Error.Set(serverSpan, true)
		serverSpan.LogFields(log.String("event", "error"), log.String("message", err.Error()))

	} else {
		serverSpan.SetTag(GRPCStatusCode, codes.OK)
	}

	serverSpan.Finish()
}
