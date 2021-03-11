package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/razorpay/metro/pkg/logger"

	"github.com/razorpay/metro/internal/subscriber"
	"github.com/razorpay/metro/internal/subscription"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

type pushStream struct {
	ctx              context.Context
	nodeID           string
	subcriptionName  string
	subscriptionCore subscription.ICore
	subscriberCore   subscriber.ICore
	responseChan     chan metrov1.PullResponse
	stopCh           chan struct{}
	doneCh           chan struct{}
}

func NewPushStream(nodeID string, subName string, subscriptionCore subscription.ICore, subscriberCore subscriber.ICore) *pushStream {
	return &pushStream{nodeID: nodeID, subcriptionName: subName, subscriptionCore: subscriptionCore, subscriberCore: subscriberCore}
}

// Start reads the messages from the broker and publish them to the subscription endpoint
func (ps *pushStream) Start() error {
	subs, err := ps.subscriberCore.NewSubscriber(ps.ctx, ps.nodeID, ps.subcriptionName, 10, 0, 0)

	if err != nil {
		return err
	}

	// get subscription Model details
	_, err = ps.subscriptionCore.Get(ps.ctx, ps.subcriptionName)
	if err != nil {
		logger.Ctx(ps.ctx).Errorf("error fetching subscription: %s", err.Error())
	}

	// Read from broker and publish to response channel in a go routine
	go func() {
		for {
			select {
			case <-ps.stopCh:
				subs.Stop()
				ps.doneCh <- struct{}{}
			default:
				subs.GetRequestChannel() <- &subscriber.PullRequest{10}
				select {
				case res := <-subs.GetResponseChannel():
					ps.responseChan <- res
				}
			}
		}
	}()

	url := ""
	for {
		// read from response channel and fire a webhook
		data := <-ps.responseChan
		postBody, _ := json.Marshal(data)
		responseBody := bytes.NewBuffer(postBody)
		resp, err := http.Post(url, "application/json", responseBody)
		if err != nil {
			logger.Ctx(ps.ctx).Errorf("error posting messages to subscription url")
		}
		defer resp.Body.Close()

		//Read the response body
		_, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			logger.Ctx(ps.ctx).Errorf("error reading response body")
		}

		// Ack/Nack
		err = subs.Acknowledge(ps.ctx, &subscriber.AcknowledgeRequest{
			AckIDs: []string{""},
		})

		if err != nil {
			logger.Ctx(ps.ctx).Errorf("error in message ack")
		}
	}

	return nil
}

// Stop is used to terminate the push subscription processing
func (ps *pushStream) Stop() error {
	// Stop the pushsubscription
	ps.stopCh <-

	// wait for stop to complete
	<-ps.doneCh
	return nil
}
