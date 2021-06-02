package metro

import (
	"context"

	"github.com/razorpay/metro/internal/config"
	"github.com/razorpay/metro/service"
	openapiserver "github.com/razorpay/metro/service/openapi-server"
	"github.com/razorpay/metro/service/web"
	worker "github.com/razorpay/metro/service/worker"
)

// Component is a holder for a metro's deployable component
type Component struct {
	name    string
	cfg     interface{}
	service service.IService
}

// NewComponent returns a new instance of a metro service component
func NewComponent(ctx context.Context, component string, cfg config.Config) (*Component, error) {
	var svc service.IService
	var config interface{}

	switch component {
	case Web:
		svc = web.NewService(ctx, &cfg.Admin, &cfg.Web, &cfg.Registry)
		config = cfg.Web
	case Worker:
		svc = worker.NewService(ctx, &cfg.Worker, &cfg.Registry)
		config = cfg.Worker
	case OpenAPIServer:
		svc = openapiserver.NewService(ctx, &cfg.OpenAPIServer)
	}
	return &Component{
		cfg:     config,
		name:    component,
		service: svc,
	}, nil
}

// Start a metro component
func (c *Component) Start() error {
	return c.service.Start()
}

// Stop a metro component
func (c *Component) Stop() error {
	return c.service.Stop()
}
