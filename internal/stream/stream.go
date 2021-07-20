package stream

import (
	"bytes"
	"context"
	"encoding/json"
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
	subscription     *subscription.Model
	subscriptionCore subscription.ICore
	subscriberCore   subscriber.ICore
	subs             subscriber.ISubscriber
	httpClient       *http.Client
	doneCh           chan struct{}
}

// Start reads the messages from the broker and publish them to the subscription endpoint
func (ps *PushStream) Start() error {
	defer close(ps.doneCh)

	var (
		err error
		// init these channels and pass to subscriber
		// the lifecycle of these channels should be maintained by the user
		subscriberRequestCh = make(chan *subscriber.PullRequest)
		subscriberAckCh     = make(chan *subscriber.AckMessage)
		subscriberModAckCh  = make(chan *subscriber.ModAckMessage)
	)

	ps.subs, err = ps.subscriberCore.NewSubscriber(ps.ctx, ps.nodeID, ps.subscription, 100, 50, 0,
		subscriberRequestCh, subscriberAckCh, subscriberModAckCh)
	if err != nil {
		logger.Ctx(ps.ctx).Errorw("worker: error creating subscriber", "subscription", ps.subscription.Name, "error", err.Error())
		return err
	}

	errGrp, gctx := errgroup.WithContext(ps.ctx)
	errGrp.Go(func() error {
		// Read from broker and publish to response channel in a go routine
		for {
			select {
			case <-gctx.Done():
				logger.Ctx(ps.ctx).Infow("worker: subscriber request and response stopped", ps.getLogFields())
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
					logger.Ctx(ps.ctx).Errorw("worker: error from subscriber", ps.getLogFields(), "err", err.Error())
					workerSubscriberErrors.WithLabelValues(env, ps.subscription.ExtractedTopicName, ps.subscription.Name, err.Error(), ps.subs.GetID()).Inc()
				}
			default:
				logger.Ctx(ps.ctx).Debugw("worker: sending a subscriber pull request", ps.getLogFields())
				ps.subs.GetRequestChannel() <- &subscriber.PullRequest{MaxNumOfMessages: 10}
				logger.Ctx(ps.ctx).Debugw("worker: waiting for subscriber data response", ps.getLogFields())
				data := <-ps.subs.GetResponseChannel()
				if data != nil && data.ReceivedMessages != nil && len(data.ReceivedMessages) > 0 {
					logger.Ctx(ps.ctx).Infow("worker: received response data from channel", ps.getLogFields())
					ps.processPushStreamResponse(ps.ctx, ps.subscription, data)
				}
			}
		}
	})

	return errGrp.Wait()
}

// Stop is used to terminate the push subscription processing
func (ps *PushStream) Stop() error {
	logger.Ctx(ps.ctx).Infow("worker: push stream stop invoked", "subscription", ps.subscription.Name)

	// signal to stop all go routines
	ps.cancelFunc()

	// wait for stop to complete
	<-ps.doneCh

	return nil
}

func (ps *PushStream) stopSubscriber() {
	// stop the subscriber
	if ps.subs != nil {
		logger.Ctx(ps.ctx).Infow("worker: stopping subscriber", ps.getLogFields())
		ps.subs.Stop()
	}
}

func (ps *PushStream) processPushStreamResponse(ctx context.Context, subModel *subscription.Model, data *metrov1.PullResponse) {
	logger.Ctx(ctx).Infow("worker: response", "data", data, ps.getLogFields())

	for _, message := range data.ReceivedMessages {
		logger.Ctx(ps.ctx).Infow("worker: publishing response data to subscription endpoint", ps.getLogFields())
		if message.AckId == "" {
			continue
		}

		logFields := ps.getLogFields()
		logFields["messageId"] = message.Message.MessageId
		logFields["ackId"] = message.AckId

		startTime := time.Now()
		pushRequest := newPushEndpointRequest(message, subModel.Name)
		postBody, _ := json.Marshal(pushRequest)
		postData := bytes.NewBuffer(postBody)
		req, err := http.NewRequest("POST", subModel.PushEndpoint, postData)
		if subModel.HasCredentials() {
			req.SetBasicAuth(subModel.GetCredentials().GetUsername(), subModel.GetCredentials().GetPassword())
		}
		logger.Ctx(ps.ctx).Infow("worker: posting messages to subscription url", logFields, "endpoint", subModel.PushEndpoint)
		resp, err := ps.httpClient.Do(req)
		workerPushEndpointCallsCount.WithLabelValues(env, subModel.ExtractedTopicName, subModel.ExtractedSubscriptionName, subModel.PushEndpoint, ps.subs.GetID()).Inc()
		workerPushEndpointTimeTaken.WithLabelValues(env, subModel.ExtractedTopicName, subModel.ExtractedSubscriptionName, subModel.PushEndpoint).Observe(time.Now().Sub(startTime).Seconds())
		if err != nil {
			logger.Ctx(ps.ctx).Errorw("worker: error posting messages to subscription url", logFields, "error", err.Error())
			ps.nack(ctx, message)
			return
		}

		logger.Ctx(ps.ctx).Infow("worker: push response received for subscription", "status", resp.StatusCode, logFields)
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
			logger.Ctx(ps.ctx).Errorw("worker: push response error on response io close()", "status", resp.StatusCode, logFields)
		}
	}
}

func (ps *PushStream) nack(ctx context.Context, message *metrov1.ReceivedMessage) {
	logFields := ps.getLogFields()
	logFields["messageId"] = message.Message.MessageId
	logFields["ackId"] = message.AckId

	logger.Ctx(ps.ctx).Infow("worker: sending nack request to subscriber", logFields)
	ackReq := subscriber.ParseAckID(message.AckId)
	// deadline is set to 0 for nack
	modackReq := subscriber.NewModAckMessage(ackReq, 0)
	// check for closed channel before sending request
	if ps.subs.GetModAckChannel() != nil {
		ps.subs.GetModAckChannel() <- modackReq
	}
}

func (ps *PushStream) ack(ctx context.Context, message *metrov1.ReceivedMessage) {
	logFields := ps.getLogFields()
	logFields["messageId"] = message.Message.MessageId
	logFields["ackId"] = message.AckId

	logger.Ctx(ps.ctx).Infow("worker: sending ack request to subscriber", logFields)
	ackReq := subscriber.ParseAckID(message.AckId)
	// check for closed channel before sending request
	if ps.subs.GetAckChannel() != nil {
		ps.subs.GetAckChannel() <- ackReq
	}
}

// returns a map of common fields to be logged
func (ps *PushStream) getLogFields() map[string]interface{} {
	return map[string]interface{}{
		"subscriberId": ps.subs.GetID(),
		"subscription": ps.subscription.Name,
	}
}

// NewPushStream return a push stream obj which is used for push subscriptions
func NewPushStream(ctx context.Context, nodeID string, subName string, subscriptionCore subscription.ICore, subscriberCore subscriber.ICore, config *HTTPClientConfig) *PushStream {
	pushCtx, cancelFunc := context.WithCancel(ctx)

	// get subscription Model details
	subModel, err := subscriptionCore.Get(pushCtx, subName)
	if err != nil {
		logger.Ctx(pushCtx).Errorf("error fetching subscription: %s", err.Error())
		return nil
	}

	// set http connection timeout from the subscription
	if subModel.AckDeadlineSec != 0 {
		// make sure to convert sec to milli-sec
		config.ConnectTimeoutMS = int(subModel.AckDeadlineSec) * 1e3
	}
	httpclient := NewHTTPClientWithConfig(config)

	return &PushStream{
		ctx:              pushCtx,
		cancelFunc:       cancelFunc,
		nodeID:           nodeID,
		subscription:     subModel,
		subscriptionCore: subscriptionCore,
		subscriberCore:   subscriberCore,
		doneCh:           make(chan struct{}),
		httpClient:       httpclient,
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

func newPushEndpointRequest(message *metrov1.ReceivedMessage, subscription string) *metrov1.PushEndpointRequest {
	return &metrov1.PushEndpointRequest{
		Message:      message.Message,
		Subscription: subscription,
	}
}
