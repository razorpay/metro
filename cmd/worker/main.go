package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/razorpay/metro/internal/job"

	"github.com/razorpay/metro/internal/boot"
	"github.com/razorpay/metro/internal/user"
	"github.com/razorpay/metro/pkg/health"
	healthv1 "github.com/razorpay/metro/rpc/common/health/v1"
)

func main() {
	ctx, cancel := context.WithCancel(boot.NewContext(context.Background()))
	defer cancel()

	// Define server handlers
	healthCore := health.NewCore(ctx)
	healthServer := health.NewServer(healthCore)
	healthServerHandler := healthv1.NewHealthCheckAPIServer(healthServer, nil)

	// Init http and register servers to mux
	mux := http.NewServeMux()
	mux.Handle(healthv1.HealthCheckAPIPathPrefix, healthServerHandler)

	userRepo := user.NewRepo(ctx, boot.DB)
	userCore := user.NewCore(ctx, userRepo)

	job.RegisterUserHandler(userCore)

	// Listen to port
	listener, err := net.Listen("tcp4", boot.Config.App.Port)

	if err != nil {
		log.Fatal(err)
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

	// Init app dependencies
	err = boot.InitWorker(ctx)

	if err != nil {
		log.Fatalf("failed to init api: %v", err)
	}

	c := make(chan os.Signal, 1)

	// accept graceful shutdowns when quit via SIGINT (Ctrl+C) or SIGTERM.
	// SIGKILL, SIGQUIT will not be caught.
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)

	// Block until signal is received.
	<-c
}
