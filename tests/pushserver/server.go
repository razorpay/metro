// Package pushserver provides an http server that intercepts the push requests
// from metro worker during integration and functional testing. The requests received
// can be forwarded to the test(s) where they can be validated.
package pushserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/razorpay/metro/pkg/logger"
)

// PushServer is a mock server that intercepts all push requests from metro worker
type PushServer struct {
	server         *http.Server
	ctx            context.Context
	stopChan       chan interface{}
	exitChan       chan interface{}
	handlerChanMap map[string]chan PushMessage
}

// PublishedMessage ...
type PublishedMessage struct {
	Data        []byte            `json:"data"`
	Attributes  map[string]string `json:"attributes"`
	MessageID   string            `json:"messageId`
	OrderingKey string            `json:"orderingKey"`
}

// PushBody ...
type PushBody struct {
	Message         PublishedMessage
	Subscription    string
	DeliveryAttempt int
}

// PushMessage is a struct encapsulating data to be pushed to test
type PushMessage struct {
	Request      PushBody
	RequestTime  int64
	ResponseChan chan int
}

// StartServer creates a server and runs it in a different go routine
func StartServer(ctx context.Context, handlerMap map[string]chan PushMessage) *PushServer {
	stopChan := make(chan interface{}, 1)
	errChan := make(chan interface{}, 1)
	exitChan := make(chan interface{}, 1)
	// c, _ := pubsub.NewClient(nil, "")
	// c.Topic("").EnableMessageOrdering=true
	// pubsub.SubscriptionConfig

	fmt.Println("Started server")
	ps := &PushServer{
		ctx:            ctx,
		stopChan:       stopChan,
		exitChan:       exitChan,
		handlerChanMap: handlerMap,
	}

	router := http.NewServeMux()
	router.HandleFunc("/push", ps.pushHandler)

	ps.server = &http.Server{
		Addr:    ":8077",
		Handler: router,
	}

	go func() {
		errChan <- runServer(ctx, ps.server)
	}()

	go func() {
		select {
		case err := <-errChan:
			logger.Ctx(ctx).Error("Error in push server", "error", err)
		case _ = <-stopChan:
			logger.Ctx(ctx).Info("Received signal to stop server")
		}
		ps.server.Shutdown(ctx)
		logger.Ctx(ctx).Info("Push server exited")
		exitChan <- "exited"
	}()

	return ps
}

func runServer(ctx context.Context, s *http.Server) error {
	if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Ctx(ctx).Fatalf("listen: %s\n", err)
		return err
	}
	return nil
}

func (ps *PushServer) pushHandler(w http.ResponseWriter, req *http.Request) {
	var pushData PushBody
	now := time.Now().UnixNano()
	decoder := json.NewDecoder(req.Body)
	err := decoder.Decode(&pushData)
	if err != nil {
		logger.Ctx(ps.ctx).Error("Error while parsing push request ", "error ", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	pushChan, ok := ps.handlerChanMap[pushData.Subscription]
	if !ok {
		logger.Ctx(ps.ctx).Error("did not find push channel for subscription ", pushData.Subscription)
		w.WriteHeader(http.StatusOK)
		return
	}

	responseChan := make(chan int)
	message := PushMessage{
		Request:      pushData,
		RequestTime:  now,
		ResponseChan: responseChan,
	}

	pushChan <- message
	w.WriteHeader(<-responseChan)
	close(responseChan)
	return
}

// StopServer stops the running http server and pauses the calling go routine untill shutdown
func (ps *PushServer) StopServer() {
	// Send a signal to stop
	ps.stopChan <- ""
	// Wait for the server shutdown
	<-ps.exitChan
}
