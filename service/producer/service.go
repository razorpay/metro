package producer

import (
	"context"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/razorpay/metro/internal/health"
	internalserver "github.com/razorpay/metro/internal/server"
	"github.com/razorpay/metro/pkg/messagebroker"
	healthv1 "github.com/razorpay/metro/rpc/health/v1"
	producerv1 "github.com/razorpay/metro/rpc/producer/v1"
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
func (svc *Service) Start(errChan chan<- error) {
	// Define server handlers

	healthCore, err := health.NewCore(nil) //TODO: Add checkers
	if err != nil {
		errChan <- err
	}

	mb, err := messagebroker.NewBroker(messagebroker.Kafka, &svc.config.Broker.BrokerConfig)
	if err != nil {
		errChan <- err
	}
	brokerCore, err := newCore(mb)
	if err != nil {
		errChan <- err
	}

	grpcServer, err := internalserver.StartGRPCServer(errChan, svc.config.Interfaces.API.GrpcServerAddress, func(server *grpc.Server) error {
		healthv1.RegisterHealthCheckAPIServer(server, health.NewServer(healthCore))
		producerv1.RegisterProducerServer(server, newServer(brokerCore))
		return nil
	},
		getInterceptors()...,
	)
	if err != nil {
		errChan <- err
	}

	httpServer, err := internalserver.StartHTTPServer(errChan, svc.config.Interfaces.API.HTTPServerAddress, func(mux *runtime.ServeMux) error {
		err := healthv1.RegisterHealthCheckAPIHandlerFromEndpoint(svc.ctx, mux, svc.config.Interfaces.API.GrpcServerAddress, []grpc.DialOption{grpc.WithInsecure()})
		if err != nil {
			return err
		}

		err = producerv1.RegisterProducerHandlerFromEndpoint(svc.ctx, mux, svc.config.Interfaces.API.GrpcServerAddress, []grpc.DialOption{grpc.WithInsecure()})
		if err != nil {
			return err
		}
		mux.Handle("GET", runtime.MustPattern(runtime.NewPattern(1, []int{2, 0, 2, 1}, []string{"v1", "metrics"}, "")), func(w http.ResponseWriter, r *http.Request, pathParams map[string]string) {
			promhttp.Handler().ServeHTTP(w, r)
		})
		return nil
	})
	if err != nil {
		errChan <- err
	}

	svc.grpcServer = grpcServer
	svc.httpServer = httpServer
	svc.health = healthCore
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
