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
	cfg     *config.Config
	service service.IService
}

// newServer returns a new instance of a metro service component
func NewService(service string, cfg *config.Config) (*Service, error) {
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

	serviceConfig, ok := s.cfg.Services[s.name]

	if !ok {
		errChan <- fmt.Errorf("`%v` service missing config", s.name)
	}

	s.service = s.startService(ctx, &serviceConfig, errChan)
	return errChan
}

func (s *Service) Stop() error {
	return s.service.Stop()
}

func (s *Service) startService(ctx context.Context, config *config.Service, errChan chan<- error) service.IService {
	var svc service.IService

	switch s.name {
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
