package metro

import (
	"context"
	"fmt"

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

func isValidComponent(component string) bool {
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
	cfg     *config.Config
	service service.IService
}

// NewComponent returns a new instance of a metro service component
func NewComponent(component string, cfg *config.Config) (*Component, error) {
	if isValidComponent(component) == false {
		return nil, fmt.Errorf("invalid component name input : %v", component)
	}

	return &Component{
		cfg:  cfg,
		name: component,
	}, nil
}

// Start a metro component
func (c *Component) Start(ctx context.Context) <-chan error {
	errChan := make(chan error)

	componentConfig, ok := c.cfg.Components[c.name]

	if !ok {
		errChan <- fmt.Errorf("`%v` service missing config", c.name)
	}

	c.service = c.start(ctx, &componentConfig, errChan)
	return errChan
}

// Stop a metro component
func (c *Component) Stop() error {
	return c.service.Stop()
}

func (c *Component) start(ctx context.Context, config *config.Component, errChan chan<- error) service.IService {
	var svc service.IService

	switch c.name {
	case Producer:
		svc = producer.NewService(ctx, config)
	case PushConsumer:
		svc = push_consumer.NewService(ctx, config)
	case PullConsumer:
		svc = pull_consumer.NewService(ctx, config)
	}

	go svc.Start(errChan)

	return svc
}
