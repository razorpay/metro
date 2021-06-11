package web

import (
	"context"
	"net/http"

	"github.com/razorpay/metro/internal/app"

	"github.com/razorpay/metro/internal/credentials"

	"github.com/razorpay/metro/internal/interceptors"

	"github.com/razorpay/metro/pkg/logger"

	"github.com/razorpay/metro/service/web/stream"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/health"
	"github.com/razorpay/metro/internal/project"
	"github.com/razorpay/metro/internal/publisher"
	"github.com/razorpay/metro/internal/server"
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
	ctx                context.Context
	grpcServer         *grpc.Server
	httpServer         *http.Server
	internalHTTPServer *http.Server
	health             *health.Core
	webConfig          *Config
	registryConfig     *registry.Config
	admin              *credentials.Model
}

// NewService creates an instance of new producer service
func NewService(ctx context.Context, admin *credentials.Model, webConfig *Config, registryConfig *registry.Config) *Service {
	return &Service{
		ctx:            ctx,
		webConfig:      webConfig,
		registryConfig: registryConfig,
		admin:          admin,
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

	r, err := registry.NewRegistry(svc.registryConfig)
	if err != nil {
		return err
	}

	brokerStore, berr := brokerstore.NewBrokerStore(svc.webConfig.Broker.Variant, &svc.webConfig.Broker.BrokerConfig)
	if berr != nil {
		return berr
	}

	projectCore := project.NewCore(project.NewRepo(r))

	topicCore := topic.NewCore(topic.NewRepo(r), projectCore, brokerStore)

	subscriptionCore := subscription.NewCore(subscription.NewRepo(r), projectCore, topicCore)

	credentialsCore := credentials.NewCore(credentials.NewRepo(r), projectCore)

	publisher := publisher.NewCore(brokerStore)

	streamManager := stream.NewStreamManager(svc.ctx, subscriptionCore, brokerStore, svc.webConfig.Interfaces.API.GrpcServerAddress)

	grpcServer, err := server.StartGRPCServer(
		grp,
		svc.webConfig.Interfaces.API.GrpcServerAddress,
		func(server *grpc.Server) error {
			metrov1.RegisterHealthCheckAPIServer(server, health.NewServer(healthCore))
			metrov1.RegisterPublisherServer(server, newPublisherServer(projectCore, brokerStore, topicCore, credentialsCore, publisher))
			metrov1.RegisterAdminServiceServer(server, newAdminServer(svc.admin, projectCore, subscriptionCore, topicCore, credentialsCore, brokerStore))
			metrov1.RegisterSubscriberServer(server, newSubscriberServer(projectCore, brokerStore, subscriptionCore, credentialsCore, streamManager))
			return nil
		},
		getInterceptors()...,
	)
	if err != nil {
		return err
	}

	httpServer, err := server.StartHTTPServer(
		grp,
		svc.webConfig.Interfaces.API.HTTPServerAddress,
		func(mux *runtime.ServeMux) error {
			err := metrov1.RegisterHealthCheckAPIHandlerFromEndpoint(gctx, mux, svc.webConfig.Interfaces.API.GrpcServerAddress, []grpc.DialOption{grpc.WithInsecure()})
			if err != nil {
				return err
			}

			err = metrov1.RegisterPublisherHandlerFromEndpoint(gctx, mux, svc.webConfig.Interfaces.API.GrpcServerAddress, []grpc.DialOption{grpc.WithInsecure()})
			if err != nil {
				return err
			}

			err = metrov1.RegisterAdminServiceHandlerFromEndpoint(gctx, mux, svc.webConfig.Interfaces.API.GrpcServerAddress, []grpc.DialOption{grpc.WithInsecure()})
			if err != nil {
				return err
			}

			err = metrov1.RegisterSubscriberHandlerFromEndpoint(svc.ctx, mux, svc.webConfig.Interfaces.API.GrpcServerAddress, []grpc.DialOption{grpc.WithInsecure()})
			if err != nil {
				return err
			}
			return nil
		})

	if err != nil {
		return err
	}

	internalHTTPServer, err := server.StartInternalHTTPServer(grp, svc.webConfig.Interfaces.API.InternalHTTPServerAddress)
	if err != nil {
		return err
	}

	svc.grpcServer = grpcServer
	svc.httpServer = httpServer
	svc.internalHTTPServer = internalHTTPServer
	svc.health = healthCore

	err = grp.Wait()
	if err != nil {
		logger.Ctx(gctx).Info("web service error: %s", err.Error())
	}
	return err
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

	grp.Go(func() error {
		return svc.internalHTTPServer.Shutdown(gctx)
	})

	return grp.Wait()
}

func getInterceptors() []grpc.UnaryServerInterceptor {
	// skip auth from test mode executions
	if app.IsTestMode() {
		return []grpc.UnaryServerInterceptor{}
	}

	return []grpc.UnaryServerInterceptor{
		interceptors.UnaryServerAuthInterceptor(func(ctx context.Context) (context.Context, error) {
			return ctx, nil
		}),
	}
}
