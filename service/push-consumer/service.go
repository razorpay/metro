package pushconsumer

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/razorpay/metro/internal/health"
	"github.com/razorpay/metro/pkg/leaderelection"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/registry"
	"golang.org/x/sync/errgroup"
)

// Service for push consumer
type Service struct {
	ctx        context.Context
	health     *health.Core
	config     *Config
	registry   registry.IRegistry
	candidate  *leaderelection.Candidate
	cancelFunc context.CancelFunc
	nodeID     string
	doneCh     chan bool
}

// NewService creates an instance of new push consumer service
func NewService(ctx context.Context, config *Config) *Service {
	ctx, cancel := context.WithCancel(ctx)

	return &Service{
		ctx:        ctx,
		config:     config,
		nodeID:     uuid.New().String(),
		cancelFunc: cancel,
		doneCh:     make(chan bool),
	}
}

// Start implements all the tasks for push-consumer and waits until one of the task fails
func (c *Service) Start() error {
	var err error
	grp, gctx := errgroup.WithContext(c.ctx)

	// Init the Registry
	// TODO: move to component init ?
	c.registry, err = registry.NewRegistry(c.ctx, &c.config.Registry)

	if err != nil {
		return err
	}

	// Init Leader Election
	c.candidate, err = leaderelection.New(leaderelection.Config{
		// TODO: read values from config
		Name:          "metro-push-consumer",
		Path:          "leader/election",
		LeaseDuration: 30 * time.Second,
		RenewDeadline: 20 * time.Second,
		RetryPeriod:   5 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				c.lead(ctx)
			},
			OnStoppedLeading: func() {
				c.stepDown()
			},
		},
	}, c.registry)

	if err != nil {
		return err
	}

	// Add node to the registry
	// TODO: use repo to add the node under /registry/nodes/{node_id} path

	// Run leader election
	grp.Go(func() error {
		return c.candidate.Run(gctx)
	})

	// 4. listen to leader channel, if elected as leader, act as leader

	// 5. watch all subscriptions, for any changes in subscripitons, if leader -> load rebalance

	// 6. watch for nodes, if any node goes down rebalance

	// 7. if leader renew session

	err = grp.Wait()
	c.doneCh <- true
	return err
}

// Stop the service
func (c *Service) Stop() error {
	c.cancelFunc()

	// wait until all goroutines return done
	<-c.doneCh

	return nil
}

func (c *Service) lead(ctx context.Context) {
	logger.Ctx(ctx).Infof("Node %s elected as new leader", c.nodeID)

	// Watch the Jobs/Node_id path for jobs
	c.registry.Watch("keyprefix", "/registry/nodes", func(pairs []registry.Pair) {
		logger.Ctx(ctx).Infow("watch handler called", "pairs", pairs)
	})
}

func (c *Service) stepDown() {
	logger.Ctx(c.ctx).Infof("Node %s stepping down from leader", c.nodeID)
}
