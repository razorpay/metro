package mockserver

import (
	"context"
	"fmt"
	"log"
	"net/http"
)

// MockServer is used to run a localhost server for functional testing
type MockServer struct {
	server *http.Server
	ReqCh  chan *http.Request
	ResCh  chan *http.Response
}

// Start runs the http server and exposes push route
func (ms *MockServer) Start(ctx context.Context) error {
	router := http.NewServeMux()

	// Registering the push handler
	router.HandleFunc("/push", ms.pushHandler)

	ms.server = &http.Server{
		Addr:    ":8099",
		Handler: router,
	}

	if err := ms.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen: %s\n", err)
		return err
	}

	return nil
}

// Stop to shutdown the http server
func (ms *MockServer) Stop(ctx context.Context) error {
	return ms.server.Shutdown(ctx)
}

func (ms *MockServer) pushHandler(writer http.ResponseWriter, req *http.Request) {
	fmt.Println("received req, writing to channel")
	ms.ReqCh <- req
	res := <-ms.ResCh
	res.Write(writer)
}
