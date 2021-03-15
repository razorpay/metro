package worker

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"

	"github.com/razorpay/metro/internal/subscriber"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/logger"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

// PushStream provides reads from broker and publishes messgaes for the push subscription
type PushStream struct {
	ctx              context.Context
	nodeID           string
	subcriptionName  string
	subscriptionCore subscription.ICore
	subscriberCore   subscriber.ICore
	responseChan     chan metrov1.PullResponse
	stopCh           chan struct{}
	doneCh           chan struct{}
}

// NewPushStream return a pushstream obj which is used for push subscriptions
func NewPushStream(nodeID string, subName string, subscriptionCore subscription.ICore, subscriberCore subscriber.ICore) *PushStream {
	return &PushStream{nodeID: nodeID, subcriptionName: subName, subscriptionCore: subscriptionCore, subscriberCore: subscriberCore}
}

// Start reads the messages from the broker and publish them to the subscription endpoint
func (ps *PushStream) Start() error {
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

		for _, message := range data.ReceivedMessages {
			postData := bytes.NewBuffer(message.Message.Data)
			resp, err := http.Post(url, "application/json", postData)
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
				AckIDs: []string{message.AckId},
			})

			if err != nil {
				logger.Ctx(ps.ctx).Errorf("error in message ack")
			}
		}
	}

	return nil
}

// Stop is used to terminate the push subscription processing
func (ps *PushStream) Stop() error {
	// Stop the pushsubscription
	ps.stopCh <-

	// wait for stop to complete
	<-ps.doneCh
	return nil
}
