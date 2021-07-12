package worker

import (
	"context"
	"net/http"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/razorpay/metro/internal/tasks"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/health"
	"github.com/razorpay/metro/internal/server"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
	"github.com/razorpay/metro/pkg/registry"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

// Service for worker
type Service struct {
	id                  string
	grpcServer          *grpc.Server
	httpServer          *http.Server
	internalHTTPServer  *http.Server
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
	registry, err := registry.NewRegistry(registryConfig)
	if err != nil {
		return nil, err
	}

	// init broker store
	brokerStore, err := brokerstore.NewBrokerStore(workerConfig.Broker.Variant, &workerConfig.Broker.BrokerConfig)
	if err != nil {
		return nil, err
	}

	// Init schedule manager
	scheduleManager, err := tasks.NewScheduleManager(workerID, registry, brokerStore)
	if err != nil {
		return nil, err
	}

	subscriptionManager, err := tasks.NewSubscriptionManager(
		workerID,
		registry,
		brokerStore,
		tasks.WithHTTPConfig(&workerConfig.HTTPClientConfig))

	if err != nil {
		return nil, err
	}

	return &Service{
		id:                  workerID,
		doneCh:              make(chan struct{}),
		workerConfig:        workerConfig,
		registry:            registry,
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

	workgrp, gctx := errgroup.WithContext(ctx)

	// Run the scheduler manager
	workgrp.Go(func() error {
		logger.Ctx(gctx).Infow("starting the metro worker manager")
		return svc.scheduleManager.Start(gctx)
	})

	// Run the subscription manager
	workgrp.Go(func() error {
		logger.Ctx(gctx).Infow("starting the metro worker manager")
		return svc.subscriptionManager.Start(gctx)
	})

	grpcServer, err := server.StartGRPCServer(
		workgrp,
		svc.workerConfig.Interfaces.API.GrpcServerAddress,
		func(server *grpc.Server) error {
			metrov1.RegisterStatusCheckAPIServer(server, health.NewServer(healthCore))
			return nil
		},
		getInterceptors()...,
	)
	if err != nil {
		return err
	}

	httpServer, err := server.StartHTTPServer(
		workgrp,
		svc.workerConfig.Interfaces.API.HTTPServerAddress,
		func(mux *runtime.ServeMux) error {
			err := metrov1.RegisterStatusCheckAPIHandlerFromEndpoint(gctx, mux, svc.workerConfig.Interfaces.API.GrpcServerAddress, []grpc.DialOption{grpc.WithInsecure()})
			if err != nil {
				return err
			}

			return nil
		})

	if err != nil {
		return err
	}

	internalHTTPServer, err := server.StartInternalHTTPServer(workgrp, svc.workerConfig.Interfaces.API.InternalHTTPServerAddress)
	if err != nil {
		return err
	}

	svc.grpcServer = grpcServer
	svc.httpServer = httpServer
	svc.internalHTTPServer = internalHTTPServer

	err = workgrp.Wait()
	if err != nil {
		logger.Ctx(gctx).Infof("worker service error: %s", err.Error())
	}
	return err
}

// Stop the service
func (svc *Service) Stop(ctx context.Context) {
	// Stop the subscription Manager
	svc.subscriptionManager.Stop(ctx)

	// Stop the Schedule Manager
	svc.scheduleManager.Stop(ctx)

	// signal the grpc server go routine
	svc.grpcServer.GracefulStop()

	// signal http server go routine
	err := svc.httpServer.Shutdown(ctx)
	if err != nil {
		logger.Ctx(ctx).Warnw("failed to stop worker http server", "error", err.Error())
	}

	err = svc.internalHTTPServer.Shutdown(ctx)
	if err != nil {
		logger.Ctx(ctx).Warnw("failed to stop worker internal http server", "error", err.Error())
	}

	// wait until all goroutines are done
	<-svc.doneCh
}

func getInterceptors() []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{}
}
