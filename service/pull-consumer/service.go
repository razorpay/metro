package pullconsumer

import (
	"context"

	"github.com/razorpay/metro/internal/config"
	"github.com/razorpay/metro/internal/health"
	"github.com/razorpay/metro/internal/server"
)

// Service for pull consumer
type Service struct {
	ctx    context.Context
	srv    *server.Server
	health *health.Core
	config *config.ComponentConfig
}

// NewService creates an instance of new pull consumer service
func NewService(ctx context.Context, config *config.ComponentConfig) *Service {
	return &Service{
		ctx:    ctx,
		config: config,
	}
}

// Start the service
func (c *Service) Start(errChan chan<- error) {
}

// Stop the service
func (c *Service) Stop() error {
	return nil
}
