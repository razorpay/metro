package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/razorpay/metro/internal/health"
	"github.com/razorpay/metro/internal/server"
	"google.golang.org/grpc"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"

	"github.com/razorpay/metro/internal/boot"
	healthv1 "github.com/razorpay/metro/rpc/common/health/v1"
)

func main() {
	// Initialize context
	ctx, cancel := context.WithCancel(boot.NewContext(context.Background()))
	defer cancel()

	// Init app dependencies
	env := boot.GetEnv()
	err := boot.InitApi(ctx, env)
	if err != nil {
		log.Fatalf("failed to init api: %v", err)
	}

	traceCloser, err := boot.InitTracing(ctx)
	if err != nil {
		log.Fatalf("error initializing tracer: %v", err)
	}
	defer traceCloser.Close()

	// Define server handlers
	healthCore, err := health.NewCore(nil)
	if err != nil {
		panic(err)
	}

	s, err := server.NewServer(boot.Config.App.Interfaces.Api, func(server *grpc.Server) error {
		healthv1.RegisterHealthCheckAPIServer(server, health.NewServer(healthCore))
		return nil
	}, func(mux *runtime.ServeMux) error {
		err := healthv1.RegisterHealthCheckAPIHandlerFromEndpoint(ctx, mux, boot.Config.App.Interfaces.Api.GrpcServerAddress, []grpc.DialOption{grpc.WithInsecure()})
		if err != nil {
			return err
		}

		return nil
	},
		getInterceptors()...,
	)

	// Define server handlers
	if err != nil {
		panic(err)
	}

	err = s.Start()
	if err != nil {
		panic(err)
	}

	// Handle SIGINT & SIGTERM - Shutdown gracefully
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)

	// Block until signal is received.
	<-c
	err = s.Stop(ctx, healthCore)
	if err != nil {
		panic(err)
	}
}

func getInterceptors() []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{}
}
