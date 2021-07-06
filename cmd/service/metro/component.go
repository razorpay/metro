package metro

import (
	"context"
	"github.com/razorpay/metro/internal/config"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/service"
	openapiserver "github.com/razorpay/metro/service/openapi-server"
	"github.com/razorpay/metro/service/web"
	"github.com/razorpay/metro/service/worker"
)

// Component is a holder for a metro's deployable component
type Component struct {
	name    string
	cfg     interface{}
	service service.IService
}

// NewComponent returns a new instance of a metro service component
func NewComponent(component string, cfg config.Config) (*Component, error) {
	var svc service.IService
	var config interface{}

	switch component {
	case Web:
		svc = web.NewService(&cfg.Admin, &cfg.Web, &cfg.Registry)
		config = cfg.Web
	case Worker:
		svc = worker.NewService(&cfg.Worker, &cfg.Registry)
		config = cfg.Worker
	case OpenAPIServer:
		svc = openapiserver.NewService(&cfg.OpenAPIServer)
	}
	return &Component{
		cfg:     config,
		name:    component,
		service: svc,
	}, nil
}

// Run a metro component
func (c *Component) Run(ctx context.Context) error {
	logger.Ctx(ctx).Infow("starting metro component", "name", c.name)

	go func() {
		<-ctx.Done()
		logger.Ctx(ctx).Infow("Stopping metro component", "name", c.name, "error", ctx.Err())

		// ensures that all the go routines in start are terminated gracefully
		c.service.Stop(ctx)
	}()

	return c.service.Start(ctx)
}
