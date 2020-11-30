package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/razorpay/metro/cmd/service/metro"
	"github.com/razorpay/metro/internal/boot"
	"github.com/razorpay/metro/pkg/logger"
)

var (
	serviceName *string
)

func init() {
	serviceName = flag.String("service", metro.Producer, "service to start")
}

func main() {
	// Initialize context
	ctx, cancel := context.WithCancel(boot.NewContext(context.Background()))
	defer cancel()

	// parse the cmd input
	flag.Parse()

	// Init app dependencies
	env := boot.GetEnv()
	err := boot.InitMetro(ctx, env)
	if err != nil {
		log.Fatalf("failed to init metro: %v", err)
	}

	// Shutdown tracer
	defer boot.Closer.Close()

	isValid := isValidService(*serviceName)
	if isValid == false {
		log.Fatalf("invalid service `%v` in input", *serviceName)
	}

	// Init Tracer
	traceCloser, err := boot.InitTracing(ctx)
	if err != nil {
		log.Fatalf("error initializing tracer: %v", err)
	}

	defer func() {
		err := traceCloser.Close()
		if err != nil {
			log.Fatalf("error closing tracer: %v", err)
		}
	}()

	// start the requested service
	var server *metro.Server
	server, err = metro.NewServer(*serviceName, &boot.Config)
	if err != nil {
		log.Fatalf("error creating metro server: %v", err)
	}

	server.Start(ctx)

	// Handle SIGINT & SIGTERM - Shutdown gracefully
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)

	// Block until signal is received.
	<-c

	logger.Ctx(ctx).Infow("stopping metro")

	// stop service
	boot.Logger(ctx).Info("stopping metro")
	err = server.Stop()
	if err != nil {
		panic(err)
	}

	logger.Ctx(ctx).Infow("stopped metro")
}
