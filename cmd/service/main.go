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
	componentName = flag.String("component", metro.Producer, "component to start")
}

func main() {
	// Initialize context
	ctx, cancel := context.WithCancel(boot.NewContext(context.Background()))
	defer cancel()

	// parse the cmd input
	flag.Parse()

	// component argument validation
	ok := metro.IsValidComponent(*componentName)
	if !ok {
		log.Fatalf("invalid component name input : %v", *componentName)
	}

	// read the env
	env := boot.GetEnv()

	// Init app dependencies
	err := boot.InitMetro(ctx, env, *componentName)
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
	component, err = metro.NewComponent(*componentName, &boot.ComponentConfig)

	if err != nil {
		log.Fatalf("error creating metro component: %v", err)
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
