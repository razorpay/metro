package metro

import (
	"context"

	pullconsumer "github.com/razorpay/metro/service/pull-consumer"
	pushconsumer "github.com/razorpay/metro/service/push-consumer"

	"github.com/razorpay/metro/internal/config"
	"github.com/razorpay/metro/service"
	"github.com/razorpay/metro/service/producer"
)

// Component is a holder for a metro's deployable component
type Component struct {
	name    string
	cfg     *config.ComponentConfig
	service service.IService
}

// NewComponent returns a new instance of a metro service component
func NewComponent(component string, cfg *config.ComponentConfig) (*Component, error) {
	return &Component{
		cfg:  cfg,
		name: component,
	}, nil
}

// Start a metro component
func (c *Component) Start(ctx context.Context) <-chan error {
	errChan := make(chan error)
	c.service = c.start(ctx, errChan)
	return errChan
}

// Stop a metro component
func (c *Component) Stop() error {
	return c.service.Stop()
}

func (c *Component) start(ctx context.Context, errChan chan<- error) service.IService {
	var svc service.IService

	switch c.name {
	case Producer:
		svc = producer.NewService(ctx, c.cfg)
	case PushConsumer:
		svc = pushconsumer.NewService(ctx, c.cfg)
	case PullConsumer:
		svc = pullconsumer.NewService(ctx, c.cfg)
	}

	go svc.Start(errChan)

	return svc
}
