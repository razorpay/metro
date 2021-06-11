package metro

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/razorpay/metro/internal/boot"
	"github.com/razorpay/metro/internal/config"
	configreader "github.com/razorpay/metro/pkg/config"
	"github.com/razorpay/metro/pkg/logger"
)

const (
	// Web component to which exposes APIs
	Web = "web"
	// Worker component which fires webhooks to subscribers
	Worker = "worker"
	// OpenAPIServer to server swagger docs
	OpenAPIServer = "openapi-server"
)

var validComponents = []string{Web, Worker, OpenAPIServer}
var component *Component

// isValidComponent validates if the input component is a valid metro component
// validComponents : web, worker
func isValidComponent(component string) bool {
	for _, s := range validComponents {
		if s == component {
			return true
		}
	}
	return false
}

// Init initializes all modules (logger, tracing, config, metro component)
func Init(ctx context.Context, env string, componentName string) {
	// componentName validation
	ok := isValidComponent(componentName)
	if !ok {
		log.Fatalf("invalid componentName given as input: [%v]", componentName)
	}

	log.Printf("starting component : [%v]", componentName)

	// read the componentName config for env
	var appConfig config.Config
	err := configreader.NewDefaultConfig().Load(env, &appConfig)
	if err != nil {
		log.Fatal(err)
	}

	if !ok {
		log.Fatalf("%v config missing", componentName)
	}

	if appConfig.Admin.Username == "" || appConfig.Admin.Password == "" {
		log.Fatal("admin credentials missing")
	}

	err = boot.InitMonitoring(env, appConfig.App, appConfig.Sentry, appConfig.Tracing)

	if err != nil {
		log.Fatalf("error in setting up monitoring : %v", err)
	}

	// Init the requested componentName
	component, err = NewComponent(ctx, componentName, appConfig)
	if err != nil {
		log.Fatalf("error in creating metro component : %v", err)
	}
}

// Run handles the component execution lifecycle
func Run(ctx context.Context) {
	// Shutdown monitoring
	defer func() {
		err := boot.Close()
		if err != nil {
			log.Fatalf("error closing tracer: %v", err)
		}
	}()

	// Handle error from component start
	errCh := make(chan error)

	// start the component in a go routine, Start should be implemented as a blocking call
	go func() {
		errCh <- component.Start()
	}()

	// Handle SIGINT & SIGTERM - Shutdown gracefully
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	select {
	// Block until SIGINT/SIGTERM signal is received
	case sig := <-sigCh:
		logger.Ctx(ctx).Infow("received a signal, stopping metro", "signal", sig)
	case err := <-errCh:
		logger.Ctx(ctx).Fatalw("component exited with error", "msg", err.Error())
	}

	// call stop component, it should internally clean up for all running go routines
	err := component.Stop()
	if err != nil {
		logger.Ctx(ctx).Fatalw("error in stopping metro")
	}

	logger.Ctx(ctx).Infow("stopped metro")
}
