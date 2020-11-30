package push_consumer

import (
	"context"
	"github.com/razorpay/metro/internal/health"
	"github.com/razorpay/metro/internal/server"
)

type Service struct {
	ctx    context.Context
	srv    *server.Server
	health *health.Core
}

func (c *Service) Start() {

}

func (c *Service) Stop() {

}
