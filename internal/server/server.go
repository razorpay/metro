package server

import (
	"context"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	grpcmiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpcopentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	grpcinterceptor "github.com/razorpay/metro/internal/interceptors"
	"github.com/razorpay/metro/pkg/logger"
)

type registerGrpcHandlers func(server *grpc.Server) error
type registerHTTPHandlers func(mux *runtime.ServeMux) error

// RunGRPCServer with handlers and interceptors
func RunGRPCServer(
	ctx context.Context,
	address string,
	registerGrpcHandlers registerGrpcHandlers,
	interceptors ...grpc.UnaryServerInterceptor) error {
	grpcServer, err := newGrpcServer(registerGrpcHandlers, interceptors...)
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	// wait for ctx.Done() in a goroutine and stop the server gracefully
	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	// Start gRPC server
	return grpcServer.Serve(listener)
}

// RunHTTPServer with handlers
func RunHTTPServer(ctx context.Context, address string, registerHTTPHandlers registerHTTPHandlers) error {
	httpServer, err := newHTTPServer(registerHTTPHandlers)
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	// Stop the server when context is Done
	go func() {
		<-ctx.Done()

		// create a new ctx for server shutdown with timeout
		// we are not using a cancelled context here as that returns immediately
		newServerCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		err := httpServer.Shutdown(newServerCtx)
		if err != nil {
			logger.Ctx(ctx).Infow("http server shutdown failed with err", "error", err.Error())
		}
	}()

	// Start HTTP server for gRPC gateway
	err = httpServer.Serve(listener)
	if err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// RunInternalHTTPServer with handlers
func RunInternalHTTPServer(ctx context.Context, address string) error {
	internalHTTPServer, err := newInternalServer()
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	// Stop the server when context is Done
	go func() {
		<-ctx.Done()

		// create a new ctx for server shutdown with timeout
		// we are not using a cancelled context here as that returns immediately
		newServerCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		err := internalHTTPServer.Shutdown(newServerCtx)
		if err != nil {
			logger.Ctx(ctx).Infow("http server shutdown failed with err", "error", err.Error())
		}
	}()

	// Start Internal HTTP server for metrics
	err = internalHTTPServer.Serve(listener)
	if err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
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
		grpcopentracing.UnaryServerInterceptor(grpcopentracing.WithFilterFunc(traceMethod)),
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
	marshlerOption := runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{})

	// https://grpc-ecosystem.github.io/grpc-gateway/docs/operations/annotated_context/
	metadataOption := runtime.WithMetadata(func(ctx context.Context, r *http.Request) metadata.MD {
		md := make(map[string]string)
		if method, ok := runtime.RPCMethod(ctx); ok {
			md["method"] = method
		}
		if pattern, ok := runtime.HTTPPathPattern(ctx); ok {
			md["pattern"] = pattern // this is coming as empty. have filed a bug to confirm.
		}
		if r.RequestURI != "" {
			md["uri"] = r.RequestURI
		}
		return metadata.New(md)
	})

	options := []runtime.ServeMuxOption{marshlerOption, metadataOption}
	mux := runtime.NewServeMux(options...)
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

// traceMethod - determines if opentracing should be enabled for the grpc method
func traceMethod(ctx context.Context, fullMethodName string) bool {
	donotTraceMethods := []string{ // Should be a very small list
		"/google.pubsub.v1.StatusCheckAPI/LivenessCheck",
		"/google.pubsub.v1.StatusCheckAPI/ReadinessCheck",
	}

	for _, method := range donotTraceMethods {
		if method == fullMethodName {
			return false
		}
	}

	return true
}
