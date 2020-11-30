package metro

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/razorpay/metro/common"
	"github.com/razorpay/metro/internal/config"
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

type Server struct {
	name    string
	cfg     *config.Config
	service common.IService
}

// newServer returns a new instance of a daemon
// that represents a cadence service
func NewServer(service string, cfg *config.Config) (*Server, error) {
	if isValidService(service) == false {
		return nil, errors.New(fmt.Sprintf("invalid service name input : %v", service))
	}

	return &Server{
		cfg:  cfg,
		name: service,
	}, nil
}

func (s *Server) Start(ctx context.Context) {
	serviceConfig, ok := s.cfg.Services[s.name]

	if !ok {
		log.Fatalf("`%v` service missing config", s.name)
	}

	s.service = s.startService(ctx, &serviceConfig)
}

func (s *Server) Stop() error {
	return s.service.Stop()
}

func (s *Server) startService(ctx context.Context, config *config.Service) common.IService {
	var service common.IService

	switch s.name {
	case Producer:
		service = producer.NewService(ctx, config)
	case PushConsumer:
		service = push_consumer.NewService(ctx, config)
	case PullConsumer:
		service = pull_consumer.NewService(ctx, config)
	}

	go service.Start()

	return service
}
