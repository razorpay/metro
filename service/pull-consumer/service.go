package pull_consumer

import (
	"context"

	"github.com/razorpay/metro/internal/config"
	"github.com/razorpay/metro/internal/health"
	"github.com/razorpay/metro/internal/server"
)

type Service struct {
	ctx    context.Context
	srv    *server.Server
	health *health.Core
	config *config.ServiceConfig
}

func NewService(ctx context.Context, config *config.ServiceConfig) *Service {
	return &Service{
		ctx:    ctx,
		config: config,
	}
}

func (c *Service) Start(errChan chan<- error) {
}

func (c *Service) Stop() error {
	return nil
}
