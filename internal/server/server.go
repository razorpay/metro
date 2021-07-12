package server

import (
	"net"
	"net/http"
	"net/http/pprof"

	grpcmiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpcopentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	grpcinterceptor "github.com/razorpay/metro/internal/interceptors"
)

type registerGrpcHandlers func(server *grpc.Server) error
type registerHTTPHandlers func(mux *runtime.ServeMux) error

// StartGRPCServer with handlers and interceptors
func StartGRPCServer(
	grp *errgroup.Group,
	address string,
	registerGrpcHandlers registerGrpcHandlers,
	interceptors ...grpc.UnaryServerInterceptor) (*grpc.Server, error) {

	grpcServer, err := newGrpcServer(registerGrpcHandlers, interceptors...)
	if err != nil {
		return nil, err
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	// Start gRPC server
	grp.Go(func() error {
		return grpcServer.Serve(listener)
	})

	return grpcServer, nil
}

// StartHTTPServer with handlers
func StartHTTPServer(grp *errgroup.Group, address string, registerHTTPHandlers registerHTTPHandlers) (*http.Server, error) {
	httpServer, err := newHTTPServer(registerHTTPHandlers)
	if err != nil {
		return nil, err
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	// Start HTTP server for gRPC gateway
	grp.Go(func() error {
		err := httpServer.Serve(listener)
		if err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	})

	return httpServer, nil
}

// StartInternalHTTPServer with handlers
func StartInternalHTTPServer(grp *errgroup.Group, address string) (*http.Server, error) {

	internalHTTPServer, err := newInternalServer()
	if err != nil {
		return nil, err
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	// Start Internal HTTP server for metrics
	grp.Go(func() error {
		err := internalHTTPServer.Serve(listener)
		if err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	})

	return internalHTTPServer, err
}

func newGrpcServer(r registerGrpcHandlers, interceptors ...grpc.UnaryServerInterceptor) (*grpc.Server, error) {
	grpcprometheus.EnableHandlingTimeHistogram(func(opts *prometheus.HistogramOpts) {
		opts.Name = "grpc_server_handled_duration_seconds"
		// The buckets covers 2ms to 8.192s
		opts.Buckets = prometheus.ExponentialBuckets(0.001, 1.25, 100)
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

func newInternalServer() (*http.Server, error) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
	mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	mux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
	mux.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))
	server := http.Server{Handler: mux}

	return &server, nil
}
