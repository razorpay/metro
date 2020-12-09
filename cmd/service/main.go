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
	componentName *string
)

func init() {
	componentName = flag.String("component", metro.Producer, "service to start")
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
	defer func() {
		err := boot.Closer.Close()
		if err != nil {
			log.Fatalf("error closing tracer: %v", err)
		}
	}()

	// start the requested component
	var component *metro.Component
	component, err = metro.NewComponent(*componentName, &boot.Config)
	if err != nil {
		log.Fatalf("error creating metro server: %v", err)
	}

	errChan := component.Start(ctx)

	// Handle SIGINT & SIGTERM - Shutdown gracefully
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)

	// Block until signal is received or error is received in starting the component.
	select {
	case <-c:
		logger.Ctx(ctx).Infow("received signal")
	case err := <-errChan:
		logger.Ctx(ctx).Fatalw("error in starting component", "msg", err.Error())
	}

	logger.Ctx(ctx).Infow("stopping metro")

	// stop component
	err = component.Stop()
	if err != nil {
		logger.Ctx(ctx).Fatalw("error in stopping metro")

	}

	logger.Ctx(ctx).Infow("stopped metro")
}
