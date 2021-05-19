package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	"github.com/razorpay/metro/internal/subscriber"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/logger"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"golang.org/x/sync/errgroup"
)

// PushStream provides reads from broker and publishes messages for the push subscription
type PushStream struct {
	ctx              context.Context
	cancelFunc       func()
	nodeID           string
	subscriptionName string
	subscriptionCore subscription.ICore
	subscriberCore   subscriber.ICore
	subs             subscriber.ISubscriber
	httpClient       *http.Client
	doneCh           chan struct{}
	notifyCh         chan error
}

// Start reads the messages from the broker and publish them to the subscription endpoint
func (ps *PushStream) Start() error {
	defer close(ps.doneCh)

	var (
		err error
		// init these channels and pass to subscriber
		// the lifecycle of these channels should be maintain by the user
		subscriberRequestCh = make(chan *subscriber.PullRequest)
		subscriberAckCh     = make(chan *subscriber.AckMessage)
		subscriberModAckCh  = make(chan *subscriber.ModAckMessage)
	)

	// get subscription Model details
	subModel, err := ps.subscriptionCore.Get(ps.ctx, ps.subscriptionName)
	if err != nil {
		logger.Ctx(ps.ctx).Errorf("error fetching subscription: %s", err.Error())
		return err
	}

	ps.subs, err = ps.subscriberCore.NewSubscriber(ps.ctx, ps.nodeID, ps.subscriptionName, 100, 50, 0,
		subscriberRequestCh, subscriberAckCh, subscriberModAckCh)
	if err != nil {
		logger.Ctx(ps.ctx).Errorw("worker: error creating subscriber", "subscription", ps.subscriptionName, "subscriberId", ps.subs.GetID(), "error", err.Error())
		// notifies the worker that subscriber creation has errored out
		ps.notifyCh <- err
		return err
	}

	// notifies the worker that subscriber creation has completed
	ps.notifyCh <- nil

	errGrp, gctx := errgroup.WithContext(ps.ctx)
	errGrp.Go(func() error {
		// Read from broker and publish to response channel in a go routine
		for {
			select {
			case <-gctx.Done():
				logger.Ctx(ps.ctx).Infow("worker: subscriber request and response stopped", "subscription", ps.subscriptionName, "subscriberId", ps.subs.GetID())
				// close all subscriber channels
				close(subscriberRequestCh)
				close(subscriberAckCh)
				close(subscriberModAckCh)

				// stop the subscriber after all the send channels are closed
				ps.stopSubscriber()

				return gctx.Err()
			case err = <-ps.subs.GetErrorChannel():
				// if channel is closed, this can return with a nil error value
				if err != nil {
					logger.Ctx(ps.ctx).Errorw("worker: error from subscriber", "subscription", ps.subscriptionName, "subscriberId", ps.subs.GetID(), "err", err.Error())
					workerSubscriberErrors.WithLabelValues(env, subModel.ExtractedTopicName, subModel.ExtractedSubscriptionName, err.Error(), ps.subs.GetID()).Inc()
				}
			default:
				//logger.Ctx(ps.ctx).Debugw("worker: sending a subscriber pull request", "subscription", ps.subscriptionName, "subscriberId", ps.subs.GetID())
				ps.subs.GetRequestChannel() <- &subscriber.PullRequest{MaxNumOfMessages: 10}
				//logger.Ctx(ps.ctx).Debugw("worker: waiting for subscriber data response", "subscription", ps.subscriptionName, "subscriberId", ps.subs.GetID())
				data := <-ps.subs.GetResponseChannel()
				if data != nil && data.ReceivedMessages != nil && len(data.ReceivedMessages) > 0 {
					//logger.Ctx(ps.ctx).Infow("worker: received response data from channel", "subscription", ps.subscriptionName, "subscriberId", ps.subs.GetID())
					ps.processPushStreamResponse(ps.ctx, subModel, data)
				}
			}
		}
	})

	return errGrp.Wait()
}

// Stop is used to terminate the push subscription processing
func (ps *PushStream) Stop() error {
	logger.Ctx(ps.ctx).Infow("worker: push stream stop invoked", "subscription", ps.subscriptionName, "subscriberId", ps.subs.GetID())

	// signal to stop all go routines
	ps.cancelFunc()

	// wait for stop to complete
	<-ps.doneCh

	return nil
}

func (ps *PushStream) stopSubscriber() {
	// stop the subscriber
	if ps.subs != nil {
		ps.subs.Stop()
	}
}

func (ps *PushStream) processPushStreamResponse(ctx context.Context, subModel *subscription.Model, data *metrov1.PullResponse) {
	logger.Ctx(ctx).Infow("worker: response", "data", data, "subscription", ps.subscriptionName, "subscriberId", ps.subs.GetID())

	for _, message := range data.ReceivedMessages {
		logger.Ctx(ps.ctx).Infow("worker: publishing response data to subscription endpoint", "subscription", ps.subscriptionName, "subscriberId", ps.subs.GetID())
		if message.AckId == "" {
			continue
		}

		startTime := time.Now()
		postData := bytes.NewBuffer(message.Message.Data)
		resp, err := ps.httpClient.Post(subModel.PushEndpoint, "application/json", postData)
		workerPushEndpointCallsCount.WithLabelValues(env, subModel.ExtractedTopicName, subModel.ExtractedSubscriptionName, subModel.PushEndpoint, ps.subs.GetID()).Inc()

		timeTaken := time.Now().Sub(startTime).Seconds()
		workerPushEndpointTimeTaken.WithLabelValues(env, subModel.ExtractedTopicName, subModel.ExtractedSubscriptionName, subModel.PushEndpoint).Observe(timeTaken)
		if err != nil {
			logger.Ctx(ps.ctx).Errorw("worker: error posting messages to subscription url", "subscription", ps.subscriptionName, "subscriberId", ps.subs.GetID(), "error", err.Error())
			ps.nack(ctx, message)
			return
		}

		logger.Ctx(ps.ctx).Infow("worker: push response received for subscription", "status", resp.StatusCode, "time_taken", timeTaken, "subscription",
			ps.subscriptionName, "subscriberId", ps.subs.GetID())
		workerPushEndpointHTTPStatusCode.WithLabelValues(env, subModel.ExtractedTopicName, subModel.ExtractedSubscriptionName, subModel.PushEndpoint, fmt.Sprintf("%v", resp.StatusCode)).Inc()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			// Ack
			ps.ack(ctx, message)
			workerMessagesAckd.WithLabelValues(env, subModel.ExtractedTopicName, subModel.ExtractedSubscriptionName, subModel.PushEndpoint, ps.subs.GetID()).Inc()
		} else {
			// Nack
			ps.nack(ctx, message)
			workerMessagesNAckd.WithLabelValues(env, subModel.ExtractedTopicName, subModel.ExtractedSubscriptionName, subModel.PushEndpoint, ps.subs.GetID()).Inc()
		}

		// discard response.Body after usage and ignore errors
		_, err = io.Copy(ioutil.Discard, resp.Body)
		err = resp.Body.Close()
		if err != nil {
			logger.Ctx(ps.ctx).Errorw("worker: push response error on response io close()", "status", resp.StatusCode, "subscription", ps.subscriptionName, "subscriberId", ps.subs.GetID(), "error", err.Error())
		}

		// TODO: read response body if required by publisher later
	}
}

func (ps *PushStream) nack(ctx context.Context, message *metrov1.ReceivedMessage) {
	logger.Ctx(ps.ctx).Infow("worker: sending nack request to subscriber", "ackId", message.AckId, "subscription", ps.subscriptionName, "subscriberId", ps.subs.GetID())
	ackReq := subscriber.ParseAckID(message.AckId)
	// deadline is set to 0 for nack
	modackReq := subscriber.NewModAckMessage(ackReq, 0)
	// check for closed channel before sending request
	if ps.subs.GetModAckChannel() != nil {
		ps.subs.GetModAckChannel() <- modackReq
	}
}

func (ps *PushStream) ack(ctx context.Context, message *metrov1.ReceivedMessage) {
	logger.Ctx(ps.ctx).Infow("worker: sending ack request to subscriber", "ackId", message.AckId, "subscription", ps.subscriptionName, "subscriberId", ps.subs.GetID())
	ackReq := subscriber.ParseAckID(message.AckId)
	// check for closed channel before sending request
	if ps.subs.GetAckChannel() != nil {
		ps.subs.GetAckChannel() <- ackReq
	}
}

// NewPushStream return a push stream obj which is used for push subscriptions
func NewPushStream(ctx context.Context, nodeID string, subName string, subscriptionCore subscription.ICore, subscriberCore subscriber.ICore, config *HTTPClientConfig, notifyCh chan error) *PushStream {
	pushCtx, cancelFunc := context.WithCancel(ctx)
	return &PushStream{
		ctx:              pushCtx,
		cancelFunc:       cancelFunc,
		nodeID:           nodeID,
		subscriptionName: subName,
		subscriptionCore: subscriptionCore,
		subscriberCore:   subscriberCore,
		doneCh:           make(chan struct{}),
		httpClient:       NewHTTPClientWithConfig(config),
		notifyCh:         notifyCh,
	}
}

// NewHTTPClientWithConfig return a http client
func NewHTTPClientWithConfig(config *HTTPClientConfig) *http.Client {
	tr := &http.Transport{
		ResponseHeaderTimeout: time.Duration(config.ResponseHeaderTimeoutMS) * time.Millisecond,
		DialContext: (&net.Dialer{
			KeepAlive: time.Duration(config.ConnKeepAliveMS) * time.Millisecond,
			Timeout:   time.Duration(config.ConnectTimeoutMS) * time.Millisecond,
		}).DialContext,
		MaxIdleConns:          config.MaxAllIdleConns,
		IdleConnTimeout:       time.Duration(config.IdleConnTimeoutMS) * time.Millisecond,
		TLSHandshakeTimeout:   time.Duration(config.TLSHandshakeTimeoutMS) * time.Millisecond,
		MaxIdleConnsPerHost:   config.MaxHostIdleConns,
		ExpectContinueTimeout: time.Duration(config.ExpectContinueTimeoutMS) * time.Millisecond,
	}

	return &http.Client{Transport: tr}
}
