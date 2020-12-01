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
	config *config.Service
}

func NewService(ctx context.Context, config *config.Service) *Service {
	return &Service{
		ctx:    ctx,
		config: config,
	}
}

func (c *Service) Start() {

}

func (c *Service) Stop() error {
	return nil
}
