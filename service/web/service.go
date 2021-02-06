package web

import (
	"context"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/health"
	"github.com/razorpay/metro/internal/project"
	"github.com/razorpay/metro/internal/publisher"
	internalserver "github.com/razorpay/metro/internal/server"
	"github.com/razorpay/metro/internal/subscriber"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/internal/topic"
	"github.com/razorpay/metro/pkg/registry"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	_ "github.com/razorpay/metro/statik" // to serve openAPI static assets
	"golang.org/x/sync/errgroup"
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
	// initiates a error group
	grp, gctx := errgroup.WithContext(svc.ctx)

	// Define server handlers
	healthCore, err := health.NewCore(nil) //TODO: Add checkers
	if err != nil {
		return err
	}

	r, err := registry.NewRegistry(svc.ctx, &svc.config.Registry)
	if err != nil {
		return err
	}

	brokerStore, berr := brokerstore.NewBrokerStore(svc.config.Broker.Variant, &svc.config.Broker.BrokerConfig)
	if berr != nil {
		return berr
	}

	projectCore := project.NewCore(project.NewRepo(r))

	topicCore := topic.NewCore(topic.NewRepo(r), projectCore, brokerStore)

	subscriptionCore := subscription.NewCore(subscription.NewRepo(r), projectCore, topicCore)

	publisher := publisher.NewCore(brokerStore)

	subscriber := subscriber.NewCore(brokerStore, subscriptionCore)

	grpcServer, err := internalserver.StartGRPCServer(
		grp,
		svc.config.Interfaces.API.GrpcServerAddress,
		func(server *grpc.Server) error {
			metrov1.RegisterHealthCheckAPIServer(server, health.NewServer(healthCore))
			metrov1.RegisterPublisherServer(server, newPublisherServer(brokerStore, topicCore, publisher))
			metrov1.RegisterAdminServiceServer(server, newAdminServer(projectCore))
			metrov1.RegisterSubscriberServer(server, newSubscriberServer(subscriptionCore, subscriber))
			return nil
		},
		getInterceptors()...,
	)
	if err != nil {
		return err
	}

	httpServer, err := internalserver.StartHTTPServer(
		grp,
		svc.config.Interfaces.API.HTTPServerAddress,
		func(mux *runtime.ServeMux) error {
			err := metrov1.RegisterHealthCheckAPIHandlerFromEndpoint(gctx, mux, svc.config.Interfaces.API.GrpcServerAddress, []grpc.DialOption{grpc.WithInsecure()})
			if err != nil {
				return err
			}

			err = metrov1.RegisterPublisherHandlerFromEndpoint(gctx, mux, svc.config.Interfaces.API.GrpcServerAddress, []grpc.DialOption{grpc.WithInsecure()})
			if err != nil {
				return err
			}

			err = metrov1.RegisterAdminServiceHandlerFromEndpoint(gctx, mux, svc.config.Interfaces.API.GrpcServerAddress, []grpc.DialOption{grpc.WithInsecure()})
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

	return grp.Wait()
}

// Stop the service
func (svc *Service) Stop() error {
	grp, gctx := errgroup.WithContext(svc.ctx)

	grp.Go(func() error {
		svc.grpcServer.GracefulStop()
		return nil
	})

	grp.Go(func() error {
		return svc.httpServer.Shutdown(gctx)
	})

	return grp.Wait()
}

func getInterceptors() []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{}
}
