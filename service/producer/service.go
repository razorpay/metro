package producer

import (
	"context"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/razorpay/metro/internal/health"
	"github.com/razorpay/metro/internal/project"
	internalserver "github.com/razorpay/metro/internal/server"
	"github.com/razorpay/metro/pkg/messagebroker"
	"github.com/razorpay/metro/pkg/registry"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	_ "github.com/razorpay/metro/statik" // to serve openAPI static assets
	"google.golang.org/grpc"
)

// Service for producer
type Service struct {
	ctx        context.Context
	grpcServer *grpc.Server
	httpServer *http.Server
	health     *health.Core
	config     *Config
}

// NewService creates an instance of new producer service
func NewService(ctx context.Context, config *Config) *Service {
	return &Service{
		ctx:    ctx,
		config: config,
	}
}

// Start the service
func (svc *Service) Start() error {
	// Define server handlers

	healthCore, err := health.NewCore(nil) //TODO: Add checkers
	if err != nil {
		return err
	}

	mb, err := messagebroker.NewBroker(messagebroker.Kafka, &svc.config.Broker.BrokerConfig)
	if err != nil {
		return err
	}

	r, err := registry.NewRegistry(&svc.config.Registry)
	if err != nil {
		return err
	}

	projectCore := project.NewCore(project.NewRepo(r))

	grpcServer, err := internalserver.StartGRPCServer(svc.ctx, svc.config.Interfaces.API.GrpcServerAddress, func(server *grpc.Server) error {
		metrov1.RegisterHealthCheckAPIServer(server, health.NewServer(healthCore))
		metrov1.RegisterPublisherServer(server, newPublisherServer(mb))
		metrov1.RegisterAdminServiceServer(server, newAdminServer(projectCore))
		metrov1.RegisterSubscriberServer(server, newSubscriberServer())

		return nil
	},
		getInterceptors()...,
	)
	if err != nil {
		return err
	}

	httpServer, err := internalserver.StartHTTPServer(svc.ctx, svc.config.Interfaces.API.HTTPServerAddress, func(mux *runtime.ServeMux) error {
		err := metrov1.RegisterHealthCheckAPIHandlerFromEndpoint(svc.ctx, mux, svc.config.Interfaces.API.GrpcServerAddress, []grpc.DialOption{grpc.WithInsecure()})
		if err != nil {
			return err
		}

		err = metrov1.RegisterPublisherHandlerFromEndpoint(svc.ctx, mux, svc.config.Interfaces.API.GrpcServerAddress, []grpc.DialOption{grpc.WithInsecure()})
		if err != nil {
			return err
		}

		err = metrov1.RegisterAdminServiceHandlerFromEndpoint(svc.ctx, mux, svc.config.Interfaces.API.GrpcServerAddress, []grpc.DialOption{grpc.WithInsecure()})
		if err != nil {
			return err
		}

		err = metrov1.RegisterSubscriberHandlerFromEndpoint(svc.ctx, mux, svc.config.Interfaces.API.GrpcServerAddress, []grpc.DialOption{grpc.WithInsecure()})
		if err != nil {
			return err
		}

		mux.Handle("GET", runtime.MustPattern(runtime.NewPattern(1, []int{2, 0, 2, 1}, []string{"v1", "metrics"}, "")), func(w http.ResponseWriter, r *http.Request, pathParams map[string]string) {
			promhttp.Handler().ServeHTTP(w, r)
		})
		return nil
	})
	if err != nil {
		return err
	}

	svc.grpcServer = grpcServer
	svc.httpServer = httpServer
	svc.health = healthCore

	return nil
}

// Stop the service
func (svc *Service) Stop() error {
	svc.grpcServer.GracefulStop()
	svc.httpServer.Shutdown(svc.ctx)
	return nil
}

func getInterceptors() []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{}
}
