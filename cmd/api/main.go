package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	ottwirp "github.com/twirp-ecosystem/twirp-opentracing"
	"github.com/twitchtv/twirp"

	"github.com/razorpay/metro/internal/boot"
	"github.com/razorpay/metro/internal/hooks"
	"github.com/razorpay/metro/internal/user"
	"github.com/razorpay/metro/pkg/health"
	healthv1 "github.com/razorpay/metro/rpc/common/health/v1"
	userv1 "github.com/razorpay/metro/rpc/example/user/v1"
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
	healthCore := health.NewCore(ctx)
	healthServer := health.NewServer(healthCore)
	healthServerHandler := healthv1.NewHealthCheckAPIServer(healthServer, nil)

	userRepo := user.NewRepo(ctx, boot.DB)
	userCore := user.NewCore(ctx, userRepo)
	userServer := user.NewServer(userCore)

	userServerHandler := hooks.WithAuth(ottwirp.WithTraceContext(userv1.NewUserAPIServer(userServer, twirpHooks()), boot.Tracer))

	// Init http and register servers to mux
	mux := http.NewServeMux()
	mux.Handle(healthv1.HealthCheckAPIPathPrefix, healthServerHandler)
	mux.Handle(userv1.UserAPIPathPrefix, userServerHandler)

	// Serve the current git commit hash
	mux.HandleFunc("/commit.txt", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, boot.Config.App.GitCommitHash)
	})

	// register metric endpoint
	mux.Handle("/metrics", promhttp.Handler())

	// Listen to port
	listener, err := net.Listen("tcp4", boot.Config.App.Port)

	if err != nil {
		panic(err)
	}

	// Serve request - http.Serve
	httpServer := http.Server{
		Handler: mux,
	}

	go func() {
		if err := httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			boot.Logger(ctx).WithContext(ctx, nil).Fatalw("Failed to start http listener", map[string]interface{}{"error": err})
		}
	}()

	c := make(chan os.Signal, 1)

	// accept graceful shutdowns when quit via SIGINT (Ctrl+C) or SIGTERM.
	// SIGKILL, SIGQUIT will not be caught.
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)

	// Block until signal is received.
	<-c
	shutDown(ctx, &httpServer, healthCore)
}

// twirpHooks register common twirp hooks applicable to all endpoints.
func twirpHooks() *twirp.ServerHooks {
	return twirp.ChainHooks(
		hooks.Metric(),
		hooks.RequestID(),
		hooks.Auth(),
		hooks.Ctx())
}

// shutDown the application, gracefully
func shutDown(ctx context.Context, httpServer *http.Server, healthCore *health.Core) {
	// send unhealthy status to the healthcheck probe and let
	// it mark this pod OOR first before shutting the server down
	//logger.Ctx(ctx).Info("Marking server unhealthy")
	healthCore.MarkUnhealthy()

	// wait for ShutdownDelay seconds
	time.Sleep(time.Duration(boot.Config.App.ShutdownDelay) * time.Second)

	// Create a deadline to wait for.
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Duration(boot.Config.App.ShutdownTimeout)*time.Second)
	defer cancel()

	boot.Logger(ctx).Info("Shutting down metro api")
	err := httpServer.Shutdown(ctxWithTimeout)
	if err != nil {
		boot.Logger(ctx).Errorw("Failed to initiate shutdown", map[string]interface{}{"error": err})
	}
}
