package server

import (
	"context"
	"net"
	"net/http"

	grpcmiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpcopentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/prometheus/client_golang/prometheus"
	grpcinterceptor "github.com/razorpay/metro/internal/interceptors"
	"google.golang.org/grpc"
)

type registerGrpcHandlers func(server *grpc.Server) error
type registerHTTPHandlers func(mux *runtime.ServeMux) error

// StartGRPCServer with handlers and interceptors
func StartGRPCServer(ctx context.Context, address string, registerGrpcHandlers registerGrpcHandlers, interceptors ...grpc.UnaryServerInterceptor) (*grpc.Server, error) {
	grpcServer, err := newGrpcServer(registerGrpcHandlers, interceptors...)
	if err != nil {
		return nil, err
	}
	// Start gRPC server.
	var errChan chan<- error
	go func(chan<- error) {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			errChan <- err
		}

		err = grpcServer.Serve(listener)
		if err != nil {
			errChan <- err
		}
	}(errChan)
	return grpcServer, nil
}

// StartHTTPServer with handlers
func StartHTTPServer(ctx context.Context, address string, registerHTTPHandlers registerHTTPHandlers) (*http.Server, error) {
	httpServer, err := newHTTPServer(registerHTTPHandlers)
	if err != nil {
		return nil, err
	}
	// Start HTTP server for gRPC gateway.
	var errChan chan<- error
	go func(chan<- error) {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			errChan <- err
		}

		err = httpServer.Serve(listener)
		if err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}(errChan)
	return httpServer, nil
}

func newGrpcServer(r registerGrpcHandlers, interceptors ...grpc.UnaryServerInterceptor) (*grpc.Server, error) {
	grpcprometheus.EnableHandlingTimeHistogram(func(opts *prometheus.HistogramOpts) {
		opts.Name = "grpc_server_handled_duration_seconds"
		// The buckets covers 2ms to 8.192s
		opts.Buckets = prometheus.ExponentialBuckets(0.002, 2, 13)
	})
	defaultInterceptors := []grpc.UnaryServerInterceptor{
		// Set tags in context. These tags will be used by subsequent middlewares - logger & tracing.
		grpcctxtags.UnaryServerInterceptor(),
		// Add RZP specific tags to context.
		grpcinterceptor.UnaryServerTagInterceptor(),
		// Add tags to logger context.
		grpcinterceptor.UnaryServerLoggerInterceptor(),
		// Todo: Confirm tracing is working as expected. Jaegar integration?
		grpcopentracing.UnaryServerInterceptor(),
		// Instrument prometheus metrics for all methods. This will have a counter & histogram of latency.
		grpcprometheus.UnaryServerInterceptor,
	}
	effectiveInterceptors := append(defaultInterceptors, interceptors...)
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(
			grpcmiddleware.ChainUnaryServer(effectiveInterceptors...),
		),
	)

	err := r(grpcServer)
	if err != nil {
		return nil, err
	}

	grpcprometheus.Register(grpcServer)
	return grpcServer, nil
}

func newHTTPServer(r registerHTTPHandlers) (*http.Server, error) {
	// MarshalerOption is added so that grpc-gateway does not omit empty values - https://stackoverflow.com/a/50044963
	mux := runtime.NewServeMux(runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{}))
	err := r(mux)
	if err != nil {
		return nil, err
	}

	server := http.Server{Handler: mux}
	return &server, nil
}
