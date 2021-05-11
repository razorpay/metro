package worker

import (
	"bytes"
	"context"
	"fmt"
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
	nodeID           string
	subcriptionName  string
	subscriptionCore subscription.ICore
	subscriberCore   subscriber.ICore
	subs             subscriber.ISubscriber
	httpClient       http.Client
	stopCh           chan struct{}
	doneCh           chan struct{}
}

// Start reads the messages from the broker and publish them to the subscription endpoint
func (ps *PushStream) Start() error {
	defer close(ps.doneCh)

	var err error
	ps.subs, err = ps.subscriberCore.NewSubscriber(ps.ctx, ps.nodeID, ps.subcriptionName, 100, 50, 0)
	if err != nil {
		logger.Ctx(ps.ctx).Errorw("worker: error creating subscriber", "subscription", ps.subcriptionName, "subscriberId", ps.subs.GetID(), "error", err.Error())
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
			logger.Ctx(ps.ctx).Infow("worker: subscriber stream context done", "subscription", ps.subcriptionName, "subscriberId", ps.subs.GetID(), "error", err.Error())
		case <-ps.stopCh:
			logger.Ctx(ps.ctx).Infow("worker: subscriber stream received stop signal", "subscription", ps.subcriptionName, "subscriberId", ps.subs.GetID())
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
			case err = <-ps.subs.GetErrorChannel():
				// if channel is closed, this can return with a nil error value
				if err != nil {
					logger.Ctx(ps.ctx).Errorw("worker: error from subscriber", "subscription", ps.subcriptionName, "subscriberId", ps.subs.GetID(), "err", err.Error())
					workerSubscriberErrors.WithLabelValues(env, subModel.ExtractedTopicName, subModel.ExtractedSubscriptionName, err.Error()).Inc()
				}
			default:
				logger.Ctx(ps.ctx).Infow("worker: sending a subscriber pull request", "subscription", ps.subcriptionName, "subscriberId", ps.subs.GetID())
				ps.subs.GetRequestChannel() <- &subscriber.PullRequest{MaxNumOfMessages: 10}
			}
		}
		logger.Ctx(ps.ctx).Infow("worker: returning from pull stream go routine", "subscription", ps.subcriptionName, "subscriberId", ps.subs.GetID())
		return nil
	})

	errGrp.Go(func() error {
		// Read from response channel and process push endpoint
		for {
			select {
			case <-gctx.Done():
				return gctx.Err()
			case data := <-ps.subs.GetResponseChannel():
				logger.Ctx(ps.ctx).Infow("worker: writing subscriber data to channel", "res", data, "count", len(data.ReceivedMessages), "subscription", ps.subcriptionName, "subscriberId", ps.subs.GetID())
				if data.ReceivedMessages != nil && len(data.ReceivedMessages) > 0 {
					logger.Ctx(ps.ctx).Infow("worker: reading response data from channel", "subscription", ps.subcriptionName, "subscriberId", ps.subs.GetID())
					ps.processPushStreamResponse(ps.ctx, subModel, data)
				}
			}
		}
		logger.Ctx(ps.ctx).Infow("worker: returning from process push endpoint go routine", "subscription", ps.subcriptionName, "subscriberId", ps.subs.GetID())
		return nil
	})

	return errGrp.Wait()
}

// Stop is used to terminate the push subscription processing
func (ps *PushStream) Stop() error {
	logger.Ctx(ps.ctx).Infow("worker: push stream stop invoked", "subscription", ps.subcriptionName, "subscriberId", ps.subs.GetID())
	// Stop the push subscription
	close(ps.stopCh)

	// stop the subscriber
	if ps.subs != nil {
		ps.subs.Stop()
	}

	// wait for stop to complete
	<-ps.doneCh

	return nil
}

func (ps *PushStream) processPushStreamResponse(ctx context.Context, subModel *subscription.Model, data metrov1.PullResponse) {
	logger.Ctx(ctx).Infow("worker: response", "data", data, "subscription", ps.subcriptionName, "subscriberId", ps.subs.GetID())

	for _, message := range data.ReceivedMessages {
		logger.Ctx(ps.ctx).Infow("worker: publishing response data to subscription endpoint", "subscription", ps.subcriptionName, "subscriberId", ps.subs.GetID())
		if message.AckId == "" {
			continue
		}

		startTime := time.Now()
		postData := bytes.NewBuffer(message.Message.Data)
		resp, err := ps.httpClient.Post(subModel.PushEndpoint, "application/json", postData)
		workerPushEndpointTimeTaken.WithLabelValues(env, subModel.ExtractedTopicName, subModel.ExtractedSubscriptionName, subModel.PushEndpoint).Observe(float64(time.Since(startTime).Nanoseconds() / 1e9))
		if err != nil {
			logger.Ctx(ps.ctx).Errorw("worker: error posting messages to subscription url", "subscription", ps.subcriptionName, "subscriberId", ps.subs.GetID(), "error", err.Error())
			ps.nack(ctx, message)
			return
		}
		defer resp.Body.Close()

		logger.Ctx(ps.ctx).Infow("worker: push response received for subscription", "status", resp.StatusCode, "subscription", ps.subcriptionName, "subscriberId", ps.subs.GetID())
		workerPushEndpointHTTPStatusCode.WithLabelValues(env, subModel.ExtractedTopicName, subModel.ExtractedSubscriptionName, subModel.PushEndpoint, fmt.Sprintf("%v", resp.StatusCode)).Inc()
		if resp.StatusCode == http.StatusOK {
			// Ack
			ps.ack(ctx, message)
			workerMessagesAckd.WithLabelValues(env, subModel.ExtractedTopicName, subModel.ExtractedSubscriptionName, subModel.PushEndpoint).Inc()
		} else {
			// Nack
			ps.nack(ctx, message)
			workerMessagesNAckd.WithLabelValues(env, subModel.ExtractedTopicName, subModel.ExtractedSubscriptionName, subModel.PushEndpoint).Inc()
		}

		// TODO: read response body if required by publisher later
	}
}

func (ps *PushStream) nack(_ context.Context, message *metrov1.ReceivedMessage) {
	logger.Ctx(ps.ctx).Infow("worker: sending nack request to subscriber", "ackId", message.AckId, "subscription", ps.subcriptionName, "subscriberId", ps.subs.GetID())
	ackReq := subscriber.ParseAckID(message.AckId)
	// deadline is set to 0 for nack
	modackReq := subscriber.NewModAckMessage(ackReq, 0)
	ps.subs.GetModAckChannel() <- modackReq
}

func (ps *PushStream) ack(_ context.Context, message *metrov1.ReceivedMessage) {
	logger.Ctx(ps.ctx).Infow("worker: sending ack request to subscriber", "ackId", message.AckId, "subscription", ps.subcriptionName, "subscriberId", ps.subs.GetID())
	ackReq := subscriber.ParseAckID(message.AckId)
	ps.subs.GetAckChannel() <- ackReq
}

// NewPushStream return a push stream obj which is used for push subscriptions
func NewPushStream(ctx context.Context, nodeID string, subName string, subscriptionCore subscription.ICore, subscriberCore subscriber.ICore, config *HTTPClientConfig) *PushStream {
	return &PushStream{
		ctx:              ctx,
		nodeID:           nodeID,
		subcriptionName:  subName,
		subscriptionCore: subscriptionCore,
		subscriberCore:   subscriberCore,
		doneCh:           make(chan struct{}),
		stopCh:           make(chan struct{}),
		httpClient:       NewHTTPClientWithConfig(config),
	}
}

// NewHTTPClientWithConfig return a http client
func NewHTTPClientWithConfig(config *HTTPClientConfig) http.Client {
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

	return http.Client{Transport: tr}
}
