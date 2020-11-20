package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/razorpay/metro/internal/boot"
	"github.com/razorpay/metro/service/producer"
	"google.golang.org/grpc"
)

func main() {
	// Initialize context
	ctx, cancel := context.WithCancel(boot.NewContext(context.Background()))
	defer cancel()

	boot.Logger(ctx).Info("starting metro")

	// Init app dependencies
	env := boot.GetEnv()
	err := boot.InitProducer(ctx, env)
	if err != nil {
		log.Fatalf("failed to init metro: %v", err)
	}

	traceCloser, err := boot.InitTracing(ctx)
	if err != nil {
		log.Fatalf("error initializing tracer: %v", err)
	}
	defer traceCloser.Close()

	// initialize producer service and start it
	producerService := producer.NewService(ctx)
	producerService.Start()

	// Handle SIGINT & SIGTERM - Shutdown gracefully
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)

	// Block until signal is received.
	<-c

	boot.Logger(ctx).Info("stopping metro")
	// stop producer service
	err = producerService.Stop()
	if err != nil {
		panic(err)
	}

	boot.Logger(ctx).Info("stopped metro")
}

func getInterceptors() []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{}
}
