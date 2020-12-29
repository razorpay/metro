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
	// Producer component to which publishers publish messages
	Producer = "producer"
	// PullConsumer component from which subscribers pull messages
	PullConsumer = "pull-consumer"
	// PushConsumer component which fires webhooks to subscribers
	PushConsumer = "push-consumer"
	// OpenAPIServer to server swagger docs
	OpenAPIServer = "openapi-server"
)

var validComponents = []string{Producer, PullConsumer, PushConsumer, OpenAPIServer}
var component *Component

// isValidComponent validates if the input component is a valid metro component
// validComponents : producer, pull-consumer, push-consumer
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
		log.Fatalf("invalid componentName name input : %v", componentName)
	}

	// read the componentName config for env
	var appConfig config.Config
	err := configreader.NewDefaultConfig().Load(env, &appConfig)
	if err != nil {
		log.Fatal(err)
	}

	if !ok {
		log.Fatalf("%v config missing", componentName)
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
	// Shutdown tracer
	defer func() {
		err := boot.Closer.Close()
		if err != nil {
			log.Fatalf("error closing tracer: %v", err)
		}
	}()

	err := component.Start()
	if err != nil {
		logger.Ctx(ctx).Fatalw("error in starting component", "msg", err.Error())
		return
	}

	// Handle SIGINT & SIGTERM - Shutdown gracefully
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)

	// Block until SIGINT/SIGTERM signal is received
	sig := <-c
	logger.Ctx(ctx).Infow("received a signal, stopping metro", "signal", sig)

	// call stop component, it should internally clean up all running go routines
	err = component.Stop()
	if err != nil {
		logger.Ctx(ctx).Fatalw("error in stopping metro")
	}

	logger.Ctx(ctx).Infow("stopped metro")
}
