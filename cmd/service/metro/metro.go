package metro

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/razorpay/metro/pkg/logger"

	"github.com/razorpay/metro/internal/boot"
	"github.com/razorpay/metro/internal/config"
	config_reader "github.com/razorpay/metro/pkg/config"
)

const (
	// Producer component to which publishers publish messages
	Producer = "producer"
	// PullConsumer component from which subscribers pull messages
	PullConsumer = "pull-consumer"
	// PushConsumer component which fires webhooks to subscribers
	PushConsumer = "push-consumer"
)

var validComponents = []string{Producer, PullConsumer, PushConsumer}
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
func Init(env string, componentName string) {
	// componentName validation
	ok := isValidComponent(componentName)
	if !ok {
		log.Fatalf("invalid componentName name input : %v", componentName)
	}

	// read the componentName config for env
	var appConfig config.Config
	err := config_reader.NewDefaultConfig().Load(env, &appConfig)
	if err != nil {
		log.Fatal(err)
	}

	componentConfig, ok := appConfig[componentName]

	if !ok {
		log.Fatal("%v config missing", componentName)
	}

	err = boot.InitMonitoring(env, &componentConfig)

	if err != nil {
		log.Fatal("error in setting up monitoring : %v", err)
	}

	// Init the requested componentName
	component, err = NewComponent(componentName, &componentConfig)
	if err != nil {
		log.Fatal("error in creating metro component : %v", err)
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
	err := component.Stop()
	if err != nil {
		logger.Ctx(ctx).Fatalw("error in stopping metro")
	}

	logger.Ctx(ctx).Infow("stopped metro")
}
