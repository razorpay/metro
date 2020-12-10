package metro

import (
	"context"

	"github.com/razorpay/metro/internal/config"
	"github.com/razorpay/metro/service"
	"github.com/razorpay/metro/service/producer"
	pull_consumer "github.com/razorpay/metro/service/pull-consumer"
	push_consumer "github.com/razorpay/metro/service/push-consumer"
)

const (
	// Producer component to which publishers publish messages
	Producer = "producer"
	// PullConsumer component from which subscribers pull messages
	PullConsumer = "pull-consumer"
	// PushConsumer component which fires webhooks to subscribers
	PushConsumer = "push-consumer"
)

var validComponents = []string{Producer, PullConsumer, PushConsumer}

func IsValidComponent(component string) bool {
	for _, s := range validComponents {
		if s == component {
			return true
		}
	}
	return false
}

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
		svc = push_consumer.NewService(ctx, c.cfg)
	case PullConsumer:
		svc = pull_consumer.NewService(ctx, c.cfg)
	}

	go svc.Start(errChan)

	return svc
}
