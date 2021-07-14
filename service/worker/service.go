package worker

import (
	"context"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/health"
	"github.com/razorpay/metro/internal/server"
	"github.com/razorpay/metro/internal/tasks"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
	"github.com/razorpay/metro/pkg/registry"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// Service for worker
type Service struct {
	id                  string
	workerConfig        *Config
	scheduleManager     tasks.IManager
	subscriptionManager tasks.IManager
	doneCh              chan struct{}
	registry            registry.IRegistry
	brokerStore         brokerstore.IBrokerStore
}

// NewService creates an instance of new worker
func NewService(workerConfig *Config, registryConfig *registry.Config) (*Service, error) {
	workerID := uuid.New().String()

	// Init registry
	reg, err := registry.NewRegistry(registryConfig)
	if err != nil {
		return nil, err
	}

	// init broker store
	brokerStore, err := brokerstore.NewBrokerStore(workerConfig.Broker.Variant, &workerConfig.Broker.BrokerConfig)
	if err != nil {
		return nil, err
	}

	// Init schedule manager
	scheduleManager, err := tasks.NewScheduleManager(workerID, reg, brokerStore)
	if err != nil {
		return nil, err
	}

	subscriptionManager, err := tasks.NewSubscriptionManager(
		workerID,
		reg,
		brokerStore,
		tasks.WithHTTPConfig(&workerConfig.HTTPClientConfig))

	if err != nil {
		return nil, err
	}

	return &Service{
		id:                  workerID,
		doneCh:              make(chan struct{}),
		workerConfig:        workerConfig,
		registry:            reg,
		brokerStore:         brokerStore,
		scheduleManager:     scheduleManager,
		subscriptionManager: subscriptionManager,
	}, nil
}

// Start implements all the tasks for worker and waits until one of the task fails
func (svc *Service) Start(ctx context.Context) error {
	// close the done channel when this function returns
	defer close(svc.doneCh)

	// init registry health checker
	registryHealthChecker := health.NewRegistryHealthChecker(svc.registry)

	// init broker health checker
	admin, _ := svc.brokerStore.GetAdmin(ctx, messagebroker.AdminClientOptions{})
	brokerHealthChecker := health.NewBrokerHealthChecker(admin)

	// register broker and registry health checkers on the health server
	healthCore, err := health.NewCore(registryHealthChecker, brokerHealthChecker)
	if err != nil {
		return err
	}

	workerGroup, gctx := errgroup.WithContext(ctx)

	// Run the scheduler manager
	workerGroup.Go(func() error {
		err := svc.scheduleManager.Run(gctx)
		return err
	})

	// Run the subscription manager
	workerGroup.Go(func() error {
		err := svc.subscriptionManager.Run(gctx)
		return err
	})

	workerGroup.Go(func() error {
		err := server.RunGRPCServer(
			gctx,
			svc.workerConfig.Interfaces.API.GrpcServerAddress,
			func(server *grpc.Server) error {
				metrov1.RegisterStatusCheckAPIServer(server, health.NewServer(healthCore))
				return nil
			},
			getInterceptors()...,
		)
		return err
	})

	workerGroup.Go(func() error {
		serverErr := server.RunHTTPServer(
			gctx,
			svc.workerConfig.Interfaces.API.HTTPServerAddress,
			func(mux *runtime.ServeMux) error {
				err := metrov1.RegisterStatusCheckAPIHandlerFromEndpoint(
					gctx,
					mux,
					svc.workerConfig.Interfaces.API.GrpcServerAddress,
					[]grpc.DialOption{grpc.WithInsecure()})

				return err
			})

		return serverErr
	})

	workerGroup.Go(func() error {
		err := server.RunInternalHTTPServer(gctx, svc.workerConfig.Interfaces.API.InternalHTTPServerAddress)
		return err
	})

	err = workerGroup.Wait()
	if err != nil {
		logger.Ctx(gctx).Infof("worker service error: %s", err.Error())
	}
	return err
}

func getInterceptors() []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{}
}
