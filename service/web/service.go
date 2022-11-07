package web

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/credentials"
	"github.com/razorpay/metro/internal/health"
	"github.com/razorpay/metro/internal/interceptors"
	"github.com/razorpay/metro/internal/nodebinding"
	"github.com/razorpay/metro/internal/offset"
	"github.com/razorpay/metro/internal/project"
	"github.com/razorpay/metro/internal/publisher"
	"github.com/razorpay/metro/internal/server"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/internal/tasks"
	"github.com/razorpay/metro/internal/topic"
	"github.com/razorpay/metro/pkg/cache"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
	"github.com/razorpay/metro/pkg/registry"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	openapiserver "github.com/razorpay/metro/service/openapi-server"
	"github.com/razorpay/metro/service/web/stream"
	_ "github.com/razorpay/metro/statik" // to serve openAPI static assets
)

// Service for producer
type Service struct {
	webConfig      *Config
	registryConfig *registry.Config
	openapiConfig  *openapiserver.Config
	cacheConfig    *cache.Config
	admin          *credentials.Model
	errChan        chan error
}

// NewService creates an instance of new producer service
func NewService(admin *credentials.Model, webConfig *Config, registryConfig *registry.Config, openapiConfig *openapiserver.Config, cacheConfig *cache.Config) (*Service, error) {
	return &Service{
		webConfig:      webConfig,
		registryConfig: registryConfig,
		openapiConfig:  openapiConfig,
		cacheConfig:    cacheConfig,
		admin:          admin,
		errChan:        make(chan error),
	}, nil
}

// GetErrorChannel returns service error channel
func (svc *Service) GetErrorChannel() chan error {
	return svc.errChan
}

// Start the service
func (svc *Service) Start(ctx context.Context) error {
	// Define server handlers
	r, err := registry.NewRegistry(svc.registryConfig)
	if err != nil {
		return err
	}

	ch, err := cache.NewCache(svc.cacheConfig)
	if err != nil {
		logger.Ctx(ctx).Errorw("Failed to setup cache", "err", err.Error())
		return err
	}

	brokerStore, err := brokerstore.NewBrokerStore(svc.webConfig.Broker.Variant, &svc.webConfig.Broker.BrokerConfig)
	if err != nil {
		return err
	}

	// init registry health checker
	registryHealthChecker := health.NewRegistryHealthChecker(r)

	// init cache health checker
	// cacheHealthChecker := health.NewCacheHealthChecker(c)

	admin, _ := brokerStore.GetAdmin(ctx, messagebroker.AdminClientOptions{})
	// init broker health checker
	brokerHealthChecker := health.NewBrokerHealthChecker(admin)

	// register broker and registry health checkers on the health server
	healthCore, err := health.NewCore(registryHealthChecker, brokerHealthChecker)
	if err != nil {
		return err
	}

	projectCore := project.NewCore(project.NewRepo(r))

	nodeBindingCore := nodebinding.NewCore(nodebinding.NewRepo(r))

	topicCore := topic.NewCore(topic.NewRepo(r), projectCore, brokerStore)

	subscriptionCore := subscription.NewCore(subscription.NewRepo(r), projectCore, topicCore, brokerStore)

	credentialsCore := credentials.NewCore(credentials.NewRepo(r), projectCore)

	publisherCore := publisher.NewCore(brokerStore)

	publisherTask, err := tasks.NewPublisherTask(uuid.New().String(), r, topicCore)
	if err != nil {
		return err
	}
	go func() {
		startTime := time.Now()
		runErr := publisherTask.Run(ctx)
		if runErr != nil {
			logger.Ctx(ctx).Errorw("Error while running publisher task ", "runErr", runErr.Error())
		}
		preWarmupCacheTimeTaken.WithLabelValues(env, "PreWarmup").Observe(time.Now().Sub(startTime).Seconds())
	}()

	offsetCore := offset.NewCore(offset.NewRepo(r))

	streamManager := stream.NewStreamManager(ctx, subscriptionCore, offsetCore, brokerStore, svc.webConfig.Interfaces.API.GrpcServerAddress, topicCore)

	go func() {
		err = <-svc.errChan
		logger.Ctx(ctx).Errorw("received an error signal on web service", "error", err.Error())
		healthCore.MarkUnhealthy()
		brokerStore.FlushAllProducers(ctx)
	}()

	// initiates a error group
	grp, gctx := errgroup.WithContext(ctx)

	grp.Go(func() error {
		err := server.RunGRPCServer(
			gctx,
			svc.webConfig.Interfaces.API.GrpcServerAddress,
			func(server *grpc.Server) error {
				metrov1.RegisterStatusCheckAPIServer(server, health.NewServer(healthCore))
				metrov1.RegisterPublisherServer(server, newPublisherServer(projectCore, brokerStore, topicCore, credentialsCore, publisherCore))
				metrov1.RegisterAdminServiceServer(server, newAdminServer(svc.admin, projectCore, subscriptionCore, topicCore, credentialsCore, nodeBindingCore, brokerStore))
				metrov1.RegisterSubscriberServer(server, newSubscriberServer(projectCore, brokerStore, subscriptionCore, credentialsCore, streamManager, ch))
				return nil
			},
			getInterceptors()...,
		)

		return err
	})

	grp.Go(func() error {
		err := server.RunHTTPServer(
			gctx,
			svc.webConfig.Interfaces.API.HTTPServerAddress,
			func(mux *runtime.ServeMux) error {
				err := metrov1.RegisterStatusCheckAPIHandlerFromEndpoint(gctx, mux, svc.webConfig.Interfaces.API.GrpcServerAddress, []grpc.DialOption{grpc.WithInsecure()})
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

				err = metrov1.RegisterSubscriberHandlerFromEndpoint(gctx, mux, svc.webConfig.Interfaces.API.GrpcServerAddress, []grpc.DialOption{grpc.WithInsecure()})
				if err != nil {
					return err
				}
				return nil
			})

		return err
	})

	grp.Go(func() error {
		err := server.RunInternalHTTPServer(gctx, svc.webConfig.Interfaces.API.InternalHTTPServerAddress)
		return err
	})

	oasvc, err := openapiserver.NewService(svc.openapiConfig)
	if err != nil {
		logger.Ctx(gctx).Errorf("Failed to initialize openapi server: %s", err.Error())
	}
	grp.Go(func() error {
		return oasvc.Start(gctx)
	})

	err = grp.Wait()
	if err != nil {
		logger.Ctx(gctx).Errorf("web service error: %s", err.Error())
	}
	return err
}

func getInterceptors() []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{
		interceptors.UnaryServerAuthInterceptor(func(ctx context.Context) (context.Context, error) {
			return ctx, nil
		}),
	}
}
