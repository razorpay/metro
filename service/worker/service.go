package worker

import (
	"context"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/health"
	"github.com/razorpay/metro/internal/node"
	"github.com/razorpay/metro/internal/nodebinding"
	"github.com/razorpay/metro/internal/project"
	"github.com/razorpay/metro/internal/scheduler"
	"github.com/razorpay/metro/internal/server"
	"github.com/razorpay/metro/internal/subscriber"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/internal/tasks"
	"github.com/razorpay/metro/internal/topic"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
	"github.com/razorpay/metro/pkg/registry"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

// Service for worker
type Service struct {
	id               string
	workerConfig     *Config
	leaderTask       tasks.ITask
	subscriptionTask tasks.ITask
	doneCh           chan struct{}
	registry         registry.IRegistry
	brokerStore      brokerstore.IBrokerStore
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

	nodeCore := node.NewCore(node.NewRepo(reg))

	projectCore := project.NewCore(project.NewRepo(reg))

	topicCore := topic.NewCore(topic.NewRepo(reg), projectCore, brokerStore)

	subscriptionCore := subscription.NewCore(
		subscription.NewRepo(reg),
		projectCore,
		topicCore)

	nodeBindingCore := nodebinding.NewCore(nodebinding.NewRepo(reg))

	scheduler, err := scheduler.New(scheduler.LoadBalance)
	if err != nil {
		return nil, err
	}

	subscriberCore := subscriber.NewCore(brokerStore, subscriptionCore)

	// Init scheduler task, this schedules the subscriptions on nodes
	// Leader Task runs this task internally if node is elected as leader
	schedulerTask, err := tasks.NewSchedulerTask(
		workerID,
		reg,
		brokerStore,
		nodeCore,
		topicCore,
		nodeBindingCore,
		subscriptionCore,
		scheduler,
	)
	if err != nil {
		return nil, err
	}

	leaderTask, err := tasks.NewLeaderTask(workerID, reg, nodeCore, schedulerTask)
	if err != nil {
		return nil, err
	}

	// Init subscription task, this runs the assigned subscriptions
	subscriptionTask, err := tasks.NewSubscriptionTask(
		workerID,
		reg,
		brokerStore,
		subscriptionCore,
		nodeBindingCore,
		subscriberCore,
		tasks.WithHTTPConfig(&workerConfig.HTTPClientConfig))

	if err != nil {
		return nil, err
	}

	return &Service{
		id:               workerID,
		doneCh:           make(chan struct{}),
		workerConfig:     workerConfig,
		registry:         reg,
		brokerStore:      brokerStore,
		leaderTask:       leaderTask,
		subscriptionTask: subscriptionTask,
	}, nil
}

// Start implements all the tasks for worker and waits until one of the task fails
func (svc *Service) Start(ctx context.Context) error {
	logger.Ctx(ctx).Infow("starting metro worker")

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

	// Run the leader task
	workerGroup.Go(func() error {
		err := svc.leaderTask.Run(gctx)
		return err
	})

	// Run the subscription task
	workerGroup.Go(func() error {
		err := svc.subscriptionTask.Run(gctx)
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
