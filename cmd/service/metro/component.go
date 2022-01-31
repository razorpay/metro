package metro

import (
	"context"

	"github.com/razorpay/metro/internal/config"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/service"
	consumeplane "github.com/razorpay/metro/service/consume-plane"
	openapiserver "github.com/razorpay/metro/service/openapi-server"
	"github.com/razorpay/metro/service/web"
	"github.com/razorpay/metro/service/worker"
)

// Component is a holder for a metro's deployable component
type Component struct {
	name    string
	service service.IService
}

// NewComponent returns a new instance of a metro service component
func NewComponent(component string, cfg config.Config) (*Component, error) {
	var svc service.IService
	var err error

	switch component {
	case Web:
		svc, err = web.NewService(&cfg.Admin, &cfg.Web, &cfg.Registry, &cfg.Cache)
	case Worker:
		svc, err = worker.NewService(&cfg.Worker, &cfg.Registry, &cfg.Cache)
	case OpenAPIServer:
		svc, err = openapiserver.NewService(&cfg.OpenAPIServer)
	case ConsumePlane:
		svc, err = consumeplane.NewService(&cfg.ConsumePlane, &cfg.Registry)
	}

	if err != nil {
		return nil, err
	}

	return &Component{
		name:    component,
		service: svc,
	}, nil
}

// Run a metro component
func (c *Component) Run(ctx context.Context) error {
	logger.Ctx(ctx).Infow("starting metro component", "name", c.name)
	return c.service.Start(ctx)
}
