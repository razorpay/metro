package metro

import (
	"context"

	"github.com/razorpay/metro/internal/config"
	"github.com/razorpay/metro/service"
	openapiserver "github.com/razorpay/metro/service/openapi-server"
	"github.com/razorpay/metro/service/producer"
	pullconsumer "github.com/razorpay/metro/service/pull-consumer"
	pushconsumer "github.com/razorpay/metro/service/push-consumer"
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
	case Producer:
		svc = producer.NewService(ctx, &cfg.Producer)
		config = cfg.Producer
	case PushConsumer:
		svc = pushconsumer.NewService(ctx, &cfg.PushConsumer)
		config = cfg.PushConsumer
	case PullConsumer:
		svc = pullconsumer.NewService(ctx, &cfg.PullConsumer)
		config = cfg.PullConsumer
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
func (c *Component) Start() <-chan error {
	errChan := make(chan error)
	go c.service.Start(errChan)
	return errChan
}

// Stop a metro component
func (c *Component) Stop() error {
	return c.service.Stop()
}
