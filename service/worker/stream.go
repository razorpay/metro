package worker

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/razorpay/metro/internal/subscriber"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/logger"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"golang.org/x/sync/errgroup"
)

// PushStream provides reads from broker and publishes messgaes for the push subscription
type PushStream struct {
	ctx              context.Context
	nodeID           string
	subcriptionName  string
	subscriptionCore subscription.ICore
	subscriberCore   subscriber.ICore
	subs             subscriber.ISubscriber
	responseChan     chan metrov1.PullResponse
	stopCh           chan struct{}
	doneCh           chan struct{}
}

// Start reads the messages from the broker and publish them to the subscription endpoint
func (ps *PushStream) Start() error {
	defer close(ps.doneCh)

	var err error
	ps.subs, err = ps.subscriberCore.NewSubscriber(ps.ctx, ps.nodeID, ps.subcriptionName, 10, 50, 0)

	if err != nil {
		return err
	}

	// get subscription Model details
	subModel, err := ps.subscriptionCore.Get(ps.ctx, ps.subcriptionName)
	if err != nil {
		logger.Ctx(ps.ctx).Errorf("error fetching subscription: %s", err.Error())
		return err
	}

	errGrp, gctx := errgroup.WithContext(ps.ctx)

	errGrp.Go(func() error {
		var err error
		select {
		case <-gctx.Done():
			err = gctx.Err()
			logger.Ctx(ps.ctx).Infow("subscriber stream context done", "error", err.Error())
		case err = <-ps.subs.GetErrorChannel():
			return err
		case <-ps.stopCh:
			logger.Ctx(ps.ctx).Infow("subscriber stream reaceived stop signal")
			err = fmt.Errorf("stop channel received signal for stream, stopping")
		}

		return err
	})

	errGrp.Go(func() error {
		// Read from broker and publish to response channel in a go routine
		for {
			select {
			case <-gctx.Done():
				return gctx.Err()
			default:
				logger.Ctx(ps.ctx).Infow("sending a subscriber pull request")
				ps.subs.GetRequestChannel() <- &subscriber.PullRequest{10}
				logger.Ctx(ps.ctx).Infow("waiting for subscriber data")
				res := <-ps.subs.GetResponseChannel()
				logger.Ctx(ps.ctx).Infow("writing subscriber data to channel", "res", res)
				ps.responseChan <- res
			}
		}
		logger.Ctx(ps.ctx).Infow("returning from pull stream go routine")
		return nil
	})

	errGrp.Go(func() error {
		// read from response channel and fire a webhook
		for {
			select {
			case <-gctx.Done():
				return gctx.Err()
			default:
				logger.Ctx(ps.ctx).Infow("reading response data from channel")
				data := <-ps.responseChan
				for _, message := range data.ReceivedMessages {
					logger.Ctx(ps.ctx).Infow("publishing response data to subscription endpoint")
					postData := bytes.NewBuffer(message.Message.Data)
					resp, err := http.Post(subModel.PushEndpoint, "application/json", postData)
					if err != nil {
						logger.Ctx(ps.ctx).Errorf("error posting messages to subscription url")
						// Implement Nack
					}
					defer resp.Body.Close()

					//Read the response body
					_, err = ioutil.ReadAll(resp.Body)
					if err != nil {
						logger.Ctx(ps.ctx).Errorf("error reading response body")
					}

					// Ack/Nack
					logger.Ctx(ps.ctx).Infow("sending Ack for successful push")
					ackReq := subscriber.ParseAckID(message.AckId)
					ps.subs.GetAckChannel() <- ackReq

					if err != nil {
						logger.Ctx(ps.ctx).Errorf("error in message ack")
					}
				}
			}
		}
		logger.Ctx(ps.ctx).Infow("returning from webhook handler go routine")
		return nil
	})

	return errGrp.Wait()
}

// Stop is used to terminate the push subscription processing
func (ps *PushStream) Stop() error {
	logger.Ctx(ps.ctx).Infow("push stream stop invoked")
	// Stop the pushsubscription
	close(ps.stopCh)

	// close the response channel
	close(ps.responseChan)

	// stop the subscriber
	ps.subs.Stop()

	// wait for stop to complete
	<-ps.doneCh
	return nil
}

// NewPushStream return a pushstream obj which is used for push subscriptions
func NewPushStream(ctx context.Context, nodeID string, subName string, subscriptionCore subscription.ICore, subscriberCore subscriber.ICore) *PushStream {
	return &PushStream{
		ctx:              ctx,
		nodeID:           nodeID,
		subcriptionName:  subName,
		subscriptionCore: subscriptionCore,
		subscriberCore:   subscriberCore,
		stopCh:           make(chan struct{}),
		responseChan:     make(chan metrov1.PullResponse),
	}
}
