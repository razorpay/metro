package pushconsumer

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/razorpay/metro/internal/health"
	"github.com/razorpay/metro/pkg/leaderelection"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/registry"
)

// Service for push consumer
type Service struct {
	ctx      context.Context
	health   *health.Core
	config   *Config
	registry registry.IRegistry
	le       *leaderelection.LeaderElector
	leCancel context.CancelFunc
	nodeID   string
	wg       sync.WaitGroup
}

// NewService creates an instance of new push consumer service
func NewService(ctx context.Context, config *Config) *Service {
	return &Service{
		ctx:    ctx,
		config: config,
		nodeID: uuid.New().String(),
	}
}

// Start the service
func (c *Service) Start() error {
	var err error

	// Init the Registry
	// TODO: move to component init ?
	c.registry, err = registry.NewRegistry(&c.config.Registry)

	if err != nil {
		return err
	}

	// Init Leader Election
	c.le, err = leaderelection.NewLeaderElector(leaderelection.Config{
		// TODO: read values from config
		Name:          "metro-push-consumer",
		Path:          "leader/election",
		LeaseDuration: 3000 * time.Second,
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
	c.runLeaderElection()

	// 3. Watch the Jobs/Node_id path for jobs

	// 4. listen to leader channel, if elected as leader, act as leader

	// 5. watch all subscriptions, for any changes in subscripitons, if leader -> load rebalance

	// 6. watch for nodes, if any node goes down rebalance

	// 7. if leader renew session

	return nil
}

// Stop the service
func (c *Service) Stop() error {
	// Stop Leader Election, cancelling context will stop leader election
	logger.Ctx(c.ctx).Info("stopping leader election")
	c.leCancel()

	// wait until all goroutines return done
	c.wg.Wait()
	return nil
}

func (c *Service) lead(ctx context.Context) {
	logger.Ctx(ctx).Infof("Node %s elected as new leader", c.nodeID)
}

func (c *Service) stepDown() {
	logger.Ctx(c.ctx).Infof("Node %s stepping down from leader", c.nodeID)
}

func (c *Service) runLeaderElection() {
	ctx, cancel := context.WithCancel(c.ctx)
	c.leCancel = cancel

	c.wg.Add(1)
	go c.le.Run(ctx, &c.wg)
}
