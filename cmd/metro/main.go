package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/razorpay/metro/internal/boot"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/service/producer"
	"google.golang.org/grpc"
)

var (
	serviceName *string
)

func init() {
	serviceName = flag.String("service", Producer, "service to start")
}

func main() {
	// Initialize context
	ctx, cancel := context.WithCancel(boot.NewContext(context.Background()))
	defer cancel()

	// parse the cmd input
	flag.Parse()

	// Init app dependencies
	env := boot.GetEnv()
	err := boot.InitProducer(ctx, env)
	if err != nil {
		log.Fatalf("failed to init metro: %v", err)
	}

	// Shutdown tracer
	defer boot.Closer.Close()

	isValid := isValidService(*serviceName)
	if isValid == false {
		log.Fatalf("invalid service `%v` in input", *serviceName)
	}

	// start service
	err = startService(ctx, *serviceName)

	if err != nil {
		log.Fatalf("error starting service : %v", *serviceName, err)
	}

	// Handle SIGINT & SIGTERM - Shutdown gracefully
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)

	// Block until signal is received.
	<-c

	logger.Ctx(ctx).Infow("stopping metro")

	// stop service
	err = producerService.Stop()
	if err != nil {
		panic(err)
	}

	logger.Ctx(ctx).Infow("stopped metro")
}

func startService(ctx context.Context, serviceName string) bool {
	switch serviceName {
	case Producer:
		// initialize requested service and start it
		service := producer.NewService(ctx)
		service.Start()
		return true
	case PushConsumer:

	case PullConsumer:
	default:

	}
	return false
}

func getInterceptors() []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{}
}
