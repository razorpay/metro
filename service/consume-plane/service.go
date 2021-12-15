package consumeplane

import (
	"context"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/consumer"
	"github.com/razorpay/metro/internal/health"
	"github.com/razorpay/metro/internal/offset"
	"github.com/razorpay/metro/internal/project"
	"github.com/razorpay/metro/internal/server"
	"github.com/razorpay/metro/internal/subscriber"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/internal/topic"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
	"github.com/razorpay/metro/pkg/registry"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

// Service for producer
type Service struct {
	consumeConfig  *Config
	registryConfig *registry.Config
}

// NewService creates an instance of new producer service
func NewService(consumeConfig *Config, registryConfig *registry.Config) (*Service, error) {
	return &Service{
		consumeConfig:  consumeConfig,
		registryConfig: registryConfig,
	}, nil
}

// Start the service
func (svc *Service) Start(ctx context.Context) error {
	// Define server handlers
	r, err := registry.NewRegistry(svc.registryConfig)
	if err != nil {
		return err
	}

	brokerStore, err := brokerstore.NewBrokerStore(svc.consumeConfig.Broker.Variant, &svc.consumeConfig.Broker.BrokerConfig)
	if err != nil {
		return err
	}

	admin, _ := brokerStore.GetAdmin(ctx, messagebroker.AdminClientOptions{})
	// init broker health checker
	brokerHealthChecker := health.NewBrokerHealthChecker(admin)

	// register broker and registry health checkers on the health server
	healthCore, err := health.NewCore(brokerHealthChecker)
	if err != nil {
		return err
	}

	projectCore := project.NewCore(project.NewRepo(r))

	topicCore := topic.NewCore(topic.NewRepo(r), projectCore, brokerStore)

	subscriptionCore := subscription.NewCore(svc.consumeConfig.ReplicaCount, subscription.NewRepo(r), projectCore, topicCore)

	subscriberCore := subscriber.NewCore(brokerStore, subscriptionCore, offset.NewCore(offset.NewRepo(r)))

	mgr, err := consumer.NewLifecycleManager(ctx, svc.consumeConfig.ReplicaCount, svc.consumeConfig.OrdinalID, subscriptionCore, subscriberCore, brokerStore)
	if err != nil {
		logger.Ctx(ctx).Errorw("consumeplaneserver: error setting up lifecycle manager", "error", err.Error())
	}
	// initiates a error group
	grp, gctx := errgroup.WithContext(ctx)

	grp.Go(func() error {
		err := server.RunGRPCServer(
			gctx,
			svc.consumeConfig.Interfaces.API.GrpcServerAddress,
			func(server *grpc.Server) error {
				metrov1.RegisterStatusCheckAPIServer(server, health.NewServer(healthCore))
				metrov1.RegisterConsumePlaneServer(server, newConsumePlaneServer(brokerStore, subscriptionCore, subscriberCore, mgr))
				return nil
			},
		)

		return err
	})

	grp.Go(func() error {
		err := server.RunHTTPServer(
			gctx,
			svc.consumeConfig.Interfaces.API.HTTPServerAddress,
			func(mux *runtime.ServeMux) error {
				err := metrov1.RegisterStatusCheckAPIHandlerFromEndpoint(gctx, mux, svc.consumeConfig.Interfaces.API.GrpcServerAddress, []grpc.DialOption{grpc.WithInsecure()})
				if err != nil {
					return err
				}

				err = metrov1.RegisterConsumePlaneHandlerFromEndpoint(gctx, mux, svc.consumeConfig.Interfaces.API.GrpcServerAddress, []grpc.DialOption{grpc.WithInsecure()})
				if err != nil {
					return err
				}
				return nil
			})

		return err
	})

	grp.Go(func() error {
		err := server.RunInternalHTTPServer(gctx, svc.consumeConfig.Interfaces.API.InternalHTTPServerAddress)
		return err
	})

	err = grp.Wait()
	if err != nil {
		logger.Ctx(gctx).Errorf("web service error: %s", err.Error())
	}
	return err
}
