package metro

import (
	"context"
	"errors"
	"fmt"

	"github.com/razorpay/metro/internal/config"
	"github.com/razorpay/metro/service"
	"github.com/razorpay/metro/service/producer"
	pull_consumer "github.com/razorpay/metro/service/pull-consumer"
	push_consumer "github.com/razorpay/metro/service/push-consumer"
)

const (
	Producer     = "producer"
	PullConsumer = "pull-consumer"
	PushConsumer = "push-consumer"
)

var validServices = []string{Producer, PullConsumer, PushConsumer}

func isValidService(service string) bool {
	for _, s := range validServices {
		if s == service {
			return true
		}
	}
	return false
}

type Service struct {
	name    string
	cfg     *config.ServiceConfig
	service service.IService
}

// newServer returns a new instance of a metro service component
func NewService(service string, cfg *config.ServiceConfig) (*Service, error) {
	if isValidService(service) == false {
		return nil, errors.New(fmt.Sprintf("invalid service name input : %v", service))
	}

	return &Service{
		cfg:  cfg,
		name: service,
	}, nil
}

func (s *Service) Start(ctx context.Context) <-chan error {
	errChan := make(chan error)

	s.service = s.startService(ctx, errChan)
	return errChan
}

func (s *Service) Stop() error {
	return s.service.Stop()
}

func (s *Service) startService(ctx context.Context, errChan chan<- error) service.IService {
	var svc service.IService

	switch s.name {
	case Producer:
		svc = producer.NewService(ctx, s.cfg)
	case PushConsumer:
		svc = push_consumer.NewService(ctx, s.cfg)
	case PullConsumer:
		svc = pull_consumer.NewService(ctx, s.cfg)
	}

	go svc.Start(errChan)

	return svc
}
