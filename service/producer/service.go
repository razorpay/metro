package producer

import (
	"context"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/razorpay/metro/internal/boot"
	"github.com/razorpay/metro/internal/health"
	"github.com/razorpay/metro/internal/server"
	healthv1 "github.com/razorpay/metro/rpc/common/health/v1"
	"google.golang.org/grpc"
)

type Service struct {
	ctx    context.Context
	srv    *server.Server
	health *health.Core
}

func NewService(ctx context.Context) *Service {
	return &Service{ctx: ctx}
}

func (svc *Service) Start() {
	// Define server handlers

	healthCore, err := health.NewCore(nil)
	if err != nil {
		panic(err)
	}

	s, err := server.NewServer(boot.Config.App.Interfaces.Api, func(server *grpc.Server) error {
		healthv1.RegisterHealthCheckAPIServer(server, health.NewServer(healthCore))
		return nil
	}, func(mux *runtime.ServeMux) error {
		err := healthv1.RegisterHealthCheckAPIHandlerFromEndpoint(svc.ctx, mux, boot.Config.App.Interfaces.Api.GrpcServerAddress, []grpc.DialOption{grpc.WithInsecure()})
		if err != nil {
			return err
		}

		return nil
	},
		getInterceptors()...,
	)

	if err != nil {
		panic(err)
	}

	err = s.Start()
	if err != nil {
		panic(err)
	}
	svc.srv = s
	svc.health = healthCore
}

func (svc *Service) Stop() error {
	return svc.srv.Stop(svc.ctx, svc.health)
}

func getInterceptors() []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{}
}
