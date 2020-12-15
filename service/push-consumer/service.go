package pushconsumer

import (
	"context"

	"github.com/razorpay/metro/internal/health"
)

// Service for push consumer
type Service struct {
	ctx    context.Context
	health *health.Core
	config *Config
}

// NewService creates an instance of new push consumer service
func NewService(ctx context.Context, config *Config) *Service {
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
