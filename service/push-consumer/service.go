package pushconsumer

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/razorpay/metro/pkg/leaderelection"

	"github.com/razorpay/metro/internal/health"
	"github.com/razorpay/metro/pkg/registry"
)

// Service for push consumer
type Service struct {
	ctx      context.Context
	health   *health.Core
	config   *Config
	registry registry.Registry
	NodeId   string
}

// NewService creates an instance of new push consumer service
func NewService(ctx context.Context, config *Config) *Service {
	return &Service{
		ctx:    ctx,
		config: config,
		NodeId: uuid.New().String(),
	}
}

// Start the service
func (c *Service) Start(errChan chan<- error) {
	var err error

	// Init the Registry
	// TODO: move to component init ?
	c.registry, err = registry.NewRegistry(&c.config.Registry)

	if err != nil {
		errChan <- err
	}

	// Add node to the registry
	// TODO: use repo to add the node under /registry/nodes/{node_id} path

	// Run leader election
	go leaderelection.RunOrDie(c.ctx, leaderelection.Config{
		// TODO: read values from config
		Name:          "metro-push-consumer",
		LeaseDuration: 15 * time.Second,
		RenewDeadline: 10 * time.Second,
		RetryPeriod:   2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				c.lead(ctx)
			},
			OnStoppedLeading: func() {
				// we can do cleanup here
			},
			OnNewLeader: func(identity string) {
			},
		},
	}, c.registry)

	// 3. Watch the Jobs/Node_id path for jobs

	// 4. listen to leader channel, if elected as leader, act as leader

	// 5. watch all subscriptions, for any changes in subscripitons, if leader -> load rebalance

	// 6. watch for nodes, if any node goes down rebalance

	// 7. if leader renew session
}

// Stop the service
func (c *Service) Stop() error {
	return nil
}

func (c *Service) lead(ctx context.Context) {

}
