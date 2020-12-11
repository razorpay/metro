package pushconsumer

import (
	"context"

	"github.com/razorpay/metro/internal/config"
	"github.com/razorpay/metro/internal/health"
	"github.com/razorpay/metro/internal/server"
)

// Service for push consumer
type Service struct {
	ctx    context.Context
	srv    *server.Server
	health *health.Core
	config *config.ComponentConfig
}

// NewService creates an instance of new push consumer service
func NewService(ctx context.Context, config *config.ComponentConfig) *Service {
	return &Service{
		ctx:    ctx,
		config: config,
	}
}

// Start the service
func (c *Service) Start(errChan chan<- error) {
	//1. Register Node with Consul

	// 2. Create a channel and go routine
	//    Go routine should try to do leader election, if elected as leader, push message to channel

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
