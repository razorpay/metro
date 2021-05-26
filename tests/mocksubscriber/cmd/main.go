package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/tests/mocksubscriber/server"
)

func main() {
	ms := server.MockServer{}

	ctx := context.Background()

	// Handle error from component start
	errCh := make(chan error)

	// start the server in a go routine, Start should be implemented as a blocking call
	go func() {
		errCh <- ms.Start(ctx)
	}()

	// Handle SIGINT & SIGTERM - Shutdown gracefully
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	select {
	// Block until SIGINT/SIGTERM signal is received
	case sig := <-sigCh:
		logger.Ctx(ctx).Infow("received a signal, stopping server", "signal", sig)
	case err := <-errCh:
		logger.Ctx(ctx).Fatalw("server exited with error", "msg", err.Error())
	}

	// call stop component, it should internally clean up for all running go routines
	err := ms.Stop(ctx)
	if err != nil {
		logger.Ctx(ctx).Fatalw("error in stopping server")
	}

	logger.Ctx(ctx).Infow("server metro")
}
