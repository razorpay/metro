package main

import (
	"context"
	"github.com/razorpay/metro/common"
	"github.com/razorpay/metro/internal/config"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/service/producer"
	pull_consumer "github.com/razorpay/metro/service/pull-consumer"
	push_consumer "github.com/razorpay/metro/service/push-consumer"
	"log"
	"time"
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
	doneC   chan struct{}
	service common.IService
}

// newServer returns a new instance of a daemon
// that represents a cadence service
func newServer(service string, cfg *config.Config) common.IService {
	return &Server{
		cfg:   cfg,
		name:  service,
		doneC: make(chan struct{}),
	}
}

func (s *Server) Start() {
	if _, ok := s.cfg.Services[s.name]; !ok {
		log.Fatalf("`%v` service missing config", s)
	}
	s.service = s.startService()
}

func (s *Server) Stop() error {
	if s.service == nil {
		return nil
	}

	select {
	case <-s.doneC:
	default:
		s.service.Stop()
		select {
		case <-s.doneC:
		case <-time.After(time.Minute):
			log.Printf("timed out waiting for server %v to exit\n", s.name)
		}
	}
	return nil
}

func (s *Server) startService(ctx context.Context) common.IService {
	switch s.name {
	case Producer:
		service, err = producer.NewService(ctx)
	case PushConsumer:
		service, err = push_consumer.NewService(ctx)
	case PullConsumer:
		service, err = pull_consumer.NewService(ctx)
	}
	if err != nil {
		params.Logger.Fatal("Fail to start "+s.name+" service ", tag.Error(err))
	}

	go execute(service, s.doneC)

	return service
}

// execute runs the service in a separate go routine
func execute(s common.IService, doneC chan struct{}) {
	s.Start()
	close(doneC)
}
