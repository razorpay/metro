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
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/razorpay/metro/internal/config"
	"github.com/razorpay/metro/internal/health"
	grpcinterceptor "github.com/razorpay/metro/internal/interceptors"
	"github.com/razorpay/metro/pkg/logger"
	"google.golang.org/grpc"
)

// Server in a composition of various types of servers
type Server struct {
	config         *config.ComponentConfig
	internalServer *http.Server
	grpcServer     *grpc.Server
	httpServer     *http.Server
}

type registerGrpcHandlers func(server *grpc.Server) error
type registerHTTPHandlers func(mux *runtime.ServeMux) error

// NewServer returns the Server
func NewServer(config *config.ComponentConfig, registerGrpcHandlers registerGrpcHandlers,
	registerHTTPHandlers registerHTTPHandlers, interceptors ...grpc.UnaryServerInterceptor) (*Server, error) {
	grpcServer, err := newGrpcServer(registerGrpcHandlers, interceptors...)
	if err != nil {
		return nil, err
	}

	httpServer, err := newHTTPServer(registerHTTPHandlers)
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

// Start the servers
func (s *Server) Start(errChan chan<- error) {
	// Start gRPC server.
	go func(chan<- error) {
		listener, err := net.Listen("tcp", s.config.Interfaces.API.GrpcServerAddress)
		if err != nil {
			errChan <- err
		}

		err = s.grpcServer.Serve(listener)
		if err != nil {
			errChan <- err
		}
	}(errChan)

	// Start internal HTTP server. Used for exposing prometheus metrics.
	go func(chan<- error) {
		listener, err := net.Listen("tcp", s.config.Interfaces.API.InternalServerAddress)
		if err != nil {
			errChan <- err
		}

		err = s.internalServer.Serve(listener)
		if err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}(errChan)

	// Start HTTP server for gRPC gateway.
	go func(chan<- error) {
		listener, err := net.Listen("tcp", s.config.Interfaces.API.HTTPServerAddress)
		if err != nil {
			errChan <- err
		}

		err = s.httpServer.Serve(listener)
		if err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}(errChan)
}

// Stop the servers
func (s *Server) Stop(ctx context.Context, healthCore *health.Core) error {
	logger.Ctx(ctx).Info("marking server unhealthy")
	healthCore.MarkUnhealthy()
	time.Sleep(time.Duration(s.config.App.ShutdownDelay) * time.Second)
	s.grpcServer.GracefulStop()

	err := stopHTTPServers(ctx, []*http.Server{s.internalServer, s.httpServer}, s.config.App.ShutdownTimeout)
	return err
}

func stopHTTPServers(ctx context.Context, servers []*http.Server, timeout int) error {
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

func newInternalServer() (*http.Server, error) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	server := http.Server{Handler: mux}

	return &server, nil
}
