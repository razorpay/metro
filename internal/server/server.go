package server

import (
	"context"
	"net"
	"net/http"
	"time"

	grpcmiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpcopentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	"github.com/razorpay/metro/internal/boot"
	"github.com/razorpay/metro/internal/config"
	"github.com/razorpay/metro/internal/health"
	grpcinterceptor "github.com/razorpay/metro/internal/interceptors"
)

type Server struct {
	config         config.NetworkInterfaces
	internalServer *http.Server
	grpcServer     *grpc.Server
	httpServer     *http.Server
}

type RegisterGrpcHandlers func(server *grpc.Server) error
type RegisterHttpHandlers func(mux *runtime.ServeMux) error

func NewServer(config config.NetworkInterfaces, registerGrpcHandlers RegisterGrpcHandlers,
	registerHttpHandlers RegisterHttpHandlers, interceptors ...grpc.UnaryServerInterceptor) (*Server, error) {
	grpcServer, err := newGrpcServer(registerGrpcHandlers, interceptors...)
	if err != nil {
		return nil, err
	}

	httpServer, err := newHttpServer(registerHttpHandlers)
	if err != nil {
		return nil, err
	}

	internalServer, err := newInternalServer()
	if err != nil {
		return nil, err
	}

	return &Server{
		config:         config,
		internalServer: internalServer,
		grpcServer:     grpcServer,
		httpServer:     httpServer,
	}, nil
}

func (s *Server) Start() error {
	// Start gRPC server.
	go func() {
		listener, err := net.Listen("tcp", s.config.GrpcServerAddress)
		if err != nil {
			panic(err)
		}

		err = s.grpcServer.Serve(listener)
		if err != nil {
			panic(err)
		}
	}()

	// Start internal HTTP server. Used for exposing prometheus metrics.
	go func() {
		listener, err := net.Listen("tcp", s.config.InternalServerAddress)
		if err != nil {
			panic(err)
		}

		err = s.internalServer.Serve(listener)
		if err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()

	// Start HTTP server for gRPC gateway.
	go func() {
		listener, err := net.Listen("tcp", s.config.HttpServerAddress)
		if err != nil {
			panic(err)
		}

		err = s.httpServer.Serve(listener)
		if err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()

	return nil
}

func (s *Server) Stop(ctx context.Context, healthCore *health.Core) error {
	boot.Logger(ctx).Info("marking server unhealthy")
	healthCore.MarkUnhealthy()
	time.Sleep(time.Duration(boot.Config.App.ShutdownDelay) * time.Second)
	s.grpcServer.GracefulStop()

	err := stopHttpServers(ctx, []*http.Server{s.internalServer, s.httpServer}, boot.Config.App.ShutdownTimeout)
	return err
}

func stopHttpServers(ctx context.Context, servers []*http.Server, timeout int) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()

	var err error
	for _, s := range servers {
		e := s.Shutdown(ctx)
		if e != nil {
			err = e
		}
	}

	return err
}

func newGrpcServer(r RegisterGrpcHandlers, interceptors ...grpc.UnaryServerInterceptor) (*grpc.Server, error) {
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

func newHttpServer(r RegisterHttpHandlers) (*http.Server, error) {
	// MarshalerOption is added so that grpc-gateway does not omit empty values - https://stackoverflow.com/a/50044963
	mux := runtime.NewServeMux(runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{OrigName: true, EmitDefaults: true}))
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
	server := http.Server{Handler: mux}

	return &server, nil
}
