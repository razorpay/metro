package pullconsumer

import (
	"context"

	"github.com/razorpay/metro/internal/health"
)

// Service for pull consumer
type Service struct {
	ctx    context.Context
	health *health.Core
	config *Config
}

// NewService creates an instance of new pull consumer service
func NewService(ctx context.Context, config *Config) *Service {
	return &Service{
		ctx:    ctx,
		config: config,
	}
}

// Start the service
func (c *Service) Start() error {
	return nil
}

// Stop the service
func (c *Service) Stop() error {
	return nil
}
