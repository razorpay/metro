package stream

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/opentracing/opentracing-go"
	"golang.org/x/sync/errgroup"

	"github.com/razorpay/metro/internal/subscriber"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/logger"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
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

	// we pass a new context to subscriber because if the subscriber gets a child context of the stream context. there
	// is a race condition that subscriber exits before the stream is stopped. this causes issues as there are no
	// subscribers listening to the requests send by stream
	subscriberCtx := context.Background()
	ps.subs, err = ps.subscriberCore.NewSubscriber(subscriberCtx, ps.nodeID, ps.subscription, 100, 50, 0,
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
				logger.Ctx(ps.ctx).Infow("worker: subscriber request and response stopped", "logFields", ps.getLogFields())
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
					logger.Ctx(ps.ctx).Errorw("worker: error from subscriber", "logFields", ps.getLogFields(), "error", err.Error())
					workerSubscriberErrors.WithLabelValues(env, ps.subscription.ExtractedTopicName, ps.subscription.Name, err.Error(), ps.subs.GetID()).Inc()
				}
			default:
				ps.processMessages()
			}
		}
	})

	return errGrp.Wait()
}

func (ps *PushStream) processMessages() {
	ctx := context.Background()
	span, ctx := opentracing.StartSpanFromContext(ctx, "PushStream.ProcessMessages", opentracing.Tags{
		"subscriber":   ps.subs.GetID(),
		"subscription": ps.subscription.Name,
		"topic":        ps.subscription.Topic,
	})
	defer span.Finish()

	// Send message pull request to subsriber request channel
	logger.Ctx(ctx).Debugw("worker: sending a subscriber pull request", "logFields", ps.getLogFields())
	ps.subs.GetRequestChannel() <- (&subscriber.PullRequest{MaxNumOfMessages: 10}).WithContext(ctx)

	// wait for response data from subscriber response channel
	logger.Ctx(ctx).Debugw("worker: waiting for subscriber data response", "logFields", ps.getLogFields())
	data := <-ps.subs.GetResponseChannel()
	if data != nil && data.ReceivedMessages != nil && len(data.ReceivedMessages) > 0 {
		logger.Ctx(ctx).Infow("worker: received response data from channel", "logFields", ps.getLogFields())
		ps.processPushStreamResponse(ctx, ps.subscription, data)
	}
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
		logger.Ctx(ps.ctx).Infow("worker: stopping subscriber", "logFields", ps.getLogFields())
		ps.subs.Stop()
	}
}

func (ps *PushStream) processPushStreamResponse(ctx context.Context, subModel *subscription.Model, data *metrov1.PullResponse) {
	logger.Ctx(ctx).Infow("worker: response", "len(data)", len(data.ReceivedMessages), "logFields", ps.getLogFields())

	span, ctx := opentracing.StartSpanFromContext(ctx, "PushStream.ProcessWebhooks", opentracing.Tags{
		"subscriber":   ps.subs.GetID(),
		"subscription": ps.subscription.Name,
		"topic":        ps.subscription.Topic,
	})
	defer span.Finish()

	for _, message := range data.ReceivedMessages {
		if message.AckId == "" {
			continue
		}

		success := ps.pushMessage(ctx, subModel, message)
		if !success {
			ps.nack(ctx, message)
		} else {
			ps.ack(ctx, message)
		}
	}
}

func (ps *PushStream) pushMessage(ctx context.Context, subModel *subscription.Model, message *metrov1.ReceivedMessage) bool {
	logger.Ctx(ctx).Infow("worker: publishing response data to subscription endpoint", "logFields", ps.getLogFields())
	span, ctx := opentracing.StartSpanFromContext(ctx, "PushStream.PushMessage", opentracing.Tags{
		"subscriber":   ps.subs.GetID(),
		"subscription": ps.subscription.Name,
		"topic":        ps.subscription.Topic,
		"message_id":   message.Message.MessageId,
	})
	defer span.Finish()

	logFields := ps.getLogFields()
	logFields["messageId"] = message.Message.MessageId
	logFields["ackId"] = message.AckId

	startTime := time.Now()
	pushRequest := newPushEndpointRequest(message, subModel.Name)
	postData := getRequestBytes(pushRequest)
	req, err := http.NewRequest(http.MethodPost, subModel.PushConfig.PushEndpoint, postData)
	if subModel.HasCredentials() {
		req.SetBasicAuth(subModel.GetCredentials().GetUsername(), subModel.GetCredentials().GetPassword())
	}

	logFields["endpoint"] = subModel.PushConfig.PushEndpoint
	logger.Ctx(ctx).Infow("worker: posting messages to subscription url", "logFields", logFields)
	resp, err := ps.httpClient.Do(req)

	// log metrics
	workerPushEndpointCallsCount.WithLabelValues(env, subModel.ExtractedTopicName, subModel.ExtractedSubscriptionName, subModel.PushConfig.PushEndpoint, ps.subs.GetID()).Inc()
	workerPushEndpointTimeTaken.WithLabelValues(env, subModel.ExtractedTopicName, subModel.ExtractedSubscriptionName, subModel.PushConfig.PushEndpoint).Observe(time.Now().Sub(startTime).Seconds())

	// Process responnse
	if err != nil {
		logger.Ctx(ctx).Errorw("worker: error posting messages to subscription url", "logFields", logFields, "error", err.Error())
		return false
	}

	logger.Ctx(ps.ctx).Infow("worker: push response received for subscription", "status", resp.StatusCode, "logFields", logFields)
	workerPushEndpointHTTPStatusCode.WithLabelValues(env, subModel.ExtractedTopicName, subModel.ExtractedSubscriptionName, subModel.PushConfig.PushEndpoint, fmt.Sprintf("%v", resp.StatusCode)).Inc()

	success := false
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		success = true
	}

	// discard response.Body after usage and ignore errors
	_, err = io.Copy(ioutil.Discard, resp.Body)
	err = resp.Body.Close()
	if err != nil {
		logger.Ctx(ps.ctx).Errorw("worker: push response error on response io close()", "status", resp.StatusCode, "logFields", logFields, "error", err.Error())
	}

	return success
}

func (ps *PushStream) nack(ctx context.Context, message *metrov1.ReceivedMessage) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "PushStream.Nack", opentracing.Tags{
		"subscriber":   ps.subs.GetID(),
		"subscription": ps.subscription.Name,
		"topic":        ps.subscription.Topic,
		"message_id":   message.Message.MessageId,
	})
	defer span.Finish()

	workerMessagesNAckd.WithLabelValues(
		env,
		ps.subscription.ExtractedTopicName,
		ps.subscription.ExtractedSubscriptionName,
		ps.subscription.PushConfig.PushEndpoint,
		ps.subs.GetID(),
	).Inc()

	logFields := ps.getLogFields()
	logFields["messageId"] = message.Message.MessageId
	logFields["ackId"] = message.AckId

	logger.Ctx(ctx).Infow("worker: sending nack request to subscriber", "logFields", logFields)
	ackReq := subscriber.ParseAckID(message.AckId)
	// deadline is set to 0 for nack
	modackReq := subscriber.NewModAckMessage(ackReq, 0).WithContext(ctx)
	// check for closed channel before sending request
	if ps.subs.GetModAckChannel() != nil {
		ps.subs.GetModAckChannel() <- modackReq
	}
}

func (ps *PushStream) ack(ctx context.Context, message *metrov1.ReceivedMessage) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "PushStream.Ack", opentracing.Tags{
		"subscriber":   ps.subs.GetID(),
		"subscription": ps.subscription.Name,
		"topic":        ps.subscription.Topic,
		"message_id":   message.Message.MessageId,
	})
	defer span.Finish()

	workerMessagesAckd.WithLabelValues(
		env,
		ps.subscription.ExtractedTopicName,
		ps.subscription.ExtractedSubscriptionName,
		ps.subscription.PushConfig.PushEndpoint,
		ps.subs.GetID(),
	).Inc()

	logFields := ps.getLogFields()
	logFields["messageId"] = message.Message.MessageId
	logFields["ackId"] = message.AckId

	logger.Ctx(ctx).Infow("worker: sending ack request to subscriber", "logFields", logFields)
	ackReq := subscriber.ParseAckID(message.AckId).WithContext(ctx)
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
		logger.Ctx(pushCtx).Errorw("error fetching subscription", "error", err.Error())
		return nil
	}

	// set http connection timeout from the subscription
	if subModel.AckDeadlineSeconds != 0 {
		// make sure to convert sec to milli-sec
		config.ConnectTimeoutMS = int(subModel.AckDeadlineSeconds) * 1e3
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

// use `golang/protobuf/jsonpb` lib to marhsal/unmarhsal all proto structs
func getRequestBytes(pushRequest *metrov1.PushEndpointRequest) *bytes.Buffer {
	marshaler := jsonpb.Marshaler{
		EnumsAsInts:  false,
		EmitDefaults: false,
		Indent:       "",
		OrigName:     false,
		AnyResolver:  nil,
	}

	var b []byte
	byteBuffer := bytes.NewBuffer(b)
	marshaler.Marshal(byteBuffer, pushRequest)

	return byteBuffer
}
