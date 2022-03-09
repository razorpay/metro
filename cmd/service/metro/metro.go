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
	"github.com/razorpay/metro/pkg/encryption"
	"github.com/razorpay/metro/pkg/logger"

	"net/http"

	// blank import added for testing.
	_ "net/http/pprof"
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
	log.Printf("Setting up metro component: [%v] in env: [%v]", componentName, env)

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

	if appConfig.Encryption.Key == "" {
		log.Fatal("encryption key missing")
	}
	encryption.RegisterEncryptionKey(appConfig.Encryption.Key)

	err = boot.InitMonitoring(env, appConfig.App, appConfig.Sentry, appConfig.Tracing)

	setPprofProfiles(ctx, componentName)

	if err != nil {
		log.Fatalf("error in setting up monitoring : %v", err)
	}

	// Init the requested componentName
	component, err = NewComponent(componentName, appConfig)
	if err != nil {
		log.Fatalf("error in creating metro component : %v", err)
	}
}

// Run handles the component execution lifecycle
func Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)

	// Shutdown monitoring
	defer func() {
		err := boot.Close()
		if err != nil {
			log.Fatalf("error closing tracer: %v", err)
		}
	}()

	// Handle SIGINT & SIGTERM - Shutdown gracefully
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	// cleanup
	defer func() {
		signal.Stop(sigCh)
		cancel()
	}()

	go func() {
		sig := <-sigCh
		logger.Ctx(ctx).Infow("received a signal, stopping metro", "signal", sig)
		cancel()
	}()

	err := component.Run(ctx)
	if err != nil {
		logger.Ctx(ctx).Fatalw("component exited with error", "msg", err.Error())
	}

	logger.Ctx(ctx).Infow("stopped metro")
}

// sets up pprof profile for perfomance monitoring
func setPprofProfiles(ctx context.Context, componentName string) {
	logger.Ctx(ctx).Infow("initialising pprof profiles")
	go func() {
		myMux := http.DefaultServeMux
		if err := http.ListenAndServe("localhost:8080", myMux); err != nil {
			logger.Ctx(ctx).Fatalw("Error when starting or running %v pprof http server: %v", componentName, err)
		}
	}()
}
