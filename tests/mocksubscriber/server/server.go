package server

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/tests/message"
)

// MockServer is used to run a localhost server for functional testing
type MockServer struct {
	server *http.Server
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

func (ms *MockServer) pushHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	logger.Ctx(ctx).Infow("new http request")

	var m message.Message

	err := json.NewDecoder(req.Body).Decode(&m)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// validate checksum
	h := md5.New()
	checksum := fmt.Sprintf("%x", h.Sum(m.Data))

	if m.Checksum != checksum {
		http.Error(w, string("checksum mismatch"), http.StatusBadRequest)
		return
	}

	http.Error(w, string(m.Data), m.ResponseCode)
	return
}
