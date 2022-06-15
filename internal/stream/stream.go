package stream

import (
	"bytes"
	"context"
	"net/http"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/golang/protobuf/jsonpb"
	"github.com/opentracing/opentracing-go"
	"github.com/razorpay/metro/internal/subscriber"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/httpclient"
	"github.com/razorpay/metro/pkg/logger"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

var pullBatchSize int = 10

// PushStream provides reads from broker and publishes messages for the push subscription
type PushStream struct {
	ctx              context.Context
	cancelFunc       func()
	nodeID           string
	subscription     *subscription.Model
	subscriptionCore subscription.ICore
	subscriberCore   subscriber.ICore
	subs             subscriber.ISubscriber
	fanoutChan       chan *metrov1.ReceivedMessage
	statusChan       chan deliveryStatus
	httpClient       *http.Client
	processor        *processor
	doneCh           chan struct{}
	counter          int64
	restartChan      chan bool
	stopChan         chan bool
}

const (
	defaultTimeoutMs          int   = 100
	defaultMaxOutstandingMsgs int64 = 2
	defaultMaxOuttandingBytes int64 = 0
)

// GetRestartChannel returns the chan where restart request is received
func (ps *PushStream) GetRestartChannel() chan bool {
	return ps.restartChan
}

// GetStopChannel returns the chan where stop request is received
func (ps *PushStream) GetStopChannel() chan bool {
	return ps.stopChan
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
	ps.subs, err = ps.subscriberCore.NewSubscriber(subscriberCtx, ps.nodeID, ps.subscription, defaultTimeoutMs,
		defaultMaxOutstandingMsgs, defaultMaxOuttandingBytes, subscriberRequestCh, subscriberAckCh, subscriberModAckCh)
	if err != nil {
		ps.restartChan <- true
		logger.Ctx(ps.ctx).Errorw("worker: error creating subscriber", "subscription", ps.subscription.Name, "error", err.Error())
		return err
	}
	logger.Ctx(subscriberCtx).Infow("Setting up processor for delivering messages", "subscripiton", ps.subscription.Name, "subID", ps.subs.GetID())
	ps.processor = newProcessor(subscriberCtx, pullBatchSize, ps.fanoutChan, ps.statusChan, ps.subs.GetID(), ps.subscription, ps.httpClient)
	go ps.processor.start()
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
					logger.Ctx(ps.ctx).Errorw("worker: error from subscriber, restarting", "logFields", ps.getLogFields(), "error", err.Error())
					workerSubscriberErrors.WithLabelValues(env, ps.subscription.Topic, ps.subscription.Name, err.Error(), ps.subs.GetID()).Inc()
					if err = ps.restartSubsciber(); err != nil {
						ps.restartChan <- true
						return err
					}
				}
			case ds := <-ps.statusChan:
				logger.Ctx(ps.ctx).Infow("worker: Received response form processor for message", "msgId", ds.msg.Message.MessageId)
				if !ds.status {
					ps.nack(ps.ctx, ds.msg)
				} else {
					ps.ack(ps.ctx, ds.msg)
				}
				atomic.AddInt64(&ps.counter, -1)
			default:
				ps.processMessages()
			}
		}
	})

	return errGrp.Wait()
}

func (ps *PushStream) restartSubsciber() error {
	ps.subs.Stop()
	var err error
	ps.subs, err = ps.subscriberCore.NewSubscriber(context.Background(), ps.nodeID, ps.subscription, defaultTimeoutMs,
		defaultMaxOutstandingMsgs, defaultMaxOuttandingBytes, make(chan *subscriber.PullRequest), make(chan *subscriber.AckMessage), make(chan *subscriber.ModAckMessage))
	workerComponentRestartCount.WithLabelValues(env, "subscriber", ps.subscription.Topic, ps.subscription.Name).Inc()
	if err != nil {
		logger.Ctx(ps.ctx).Errorw("worker: error restarting subscriber", "subscription", ps.subscription.Name, "error", err.Error())
		return err
	}
	return nil
}

func (ps *PushStream) processMessages() {
	ctx := context.Background()
	span, ctx := opentracing.StartSpanFromContext(ctx, "PushStream.ProcessMessages", opentracing.Tags{
		"subscriber":   ps.subs.GetID(),
		"subscription": ps.subscription.Name,
		"topic":        ps.subscription.Topic,
	})
	defer span.Finish()

	if ps.subscription.EnableMessageOrdering {
		pullBatchSize = 1
	}

	// Send message pull request to subsriber request channel
	// logger.Ctx(ctx).Debugw("worker: sending a subscriber pull request", "logFields", ps.getLogFields())
	if ps.counter >= int64(pullBatchSize) {
		return
	}
	ps.subs.GetRequestChannel() <- (&subscriber.PullRequest{MaxNumOfMessages: int32(pullBatchSize)}).WithContext(ctx)
	// wait for response data from subscriber response channel
	// logger.Ctx(ctx).Debugw("worker: waiting for subscriber data response", "logFields", ps.getLogFields())
	data := <-ps.subs.GetResponseChannel()

	if data != nil && data.ReceivedMessages != nil && len(data.ReceivedMessages) > 0 {
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

// Restart is used to restart the push subscription processing
func (ps *PushStream) Restart(ctx context.Context) {
	logger.Ctx(ps.ctx).Infow("worker: push stream restart invoked", "subscription", ps.subscription.Name)
	err := ps.Stop()
	if err != nil {
		logger.Ctx(ctx).Errorw(
			"worker: push stream stop error",
			"subscription", ps.subscription.Name,
			"error", err.Error(),
		)
		return
	}
	go func(ctx context.Context) {
		err := ps.Start()
		if err != nil {
			logger.Ctx(ctx).Errorw(
				"worker: push stream restart error",
				"subscription", ps.subscription.Name,
				"error", err.Error(),
			)
		}
	}(ctx)
	workerComponentRestartCount.WithLabelValues(env, "stream", ps.subscription.Topic, ps.subscription.Name).Inc()
}

func (ps *PushStream) stopSubscriber() {
	// stop the subscriber
	if ps.subs != nil {
		logger.Ctx(ps.ctx).Infow("worker: stopping subscriber", "logFields", ps.getLogFields())
		ps.subs.Stop()
	}
	ps.processor.Shutdown()
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
		atomic.AddInt64(&ps.counter, 1)
		if message.AckId == "" {
			continue
		}
		logger.Ctx(ctx).Infow("Dispatching message to processor", "msgId", message.Message.MessageId, "subscription", ps.subscription.Name)
		ps.fanoutChan <- message
	}
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
		ps.subscription.Topic,
		ps.subscription.Name,
		ps.subscription.PushConfig.PushEndpoint,
		ps.subs.GetID(),
	).Inc()

	logFields := ps.getLogFields()
	logFields["messageId"] = message.Message.MessageId
	logFields["ackId"] = message.AckId

	logger.Ctx(ctx).Infow("worker: sending nack request to subscriber", "logFields", logFields)
	ackReq, err := subscriber.ParseAckID(message.AckId)
	if err != nil {
		logger.Ctx(ctx).Errorf("worker: error in parsing ackId", "error", err.Error(), "logFields", logFields)
		return
	}
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
		ps.subscription.Topic,
		ps.subscription.Name,
		ps.subscription.PushConfig.PushEndpoint,
		ps.subs.GetID(),
	).Inc()

	logFields := ps.getLogFields()
	logFields["messageId"] = message.Message.MessageId
	logFields["ackId"] = message.AckId

	logger.Ctx(ctx).Infow("worker: sending ack request to subscriber", "logFields", logFields)
	ackReq, err := subscriber.ParseAckID(message.AckId)
	if err != nil {
		logger.Ctx(ctx).Errorw("worker: error in parsing ackId", "error", err.Error(), "logFields", logFields)
		return
	}
	ackReq = ackReq.WithContext(ctx)

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
func NewPushStream(ctx context.Context, nodeID string, subName string, subscriptionCore subscription.ICore, subscriberCore subscriber.ICore, config *httpclient.Config) (*PushStream, error) {
	pushCtx, cancelFunc := context.WithCancel(ctx)
	logger.Ctx(pushCtx).Infow("worker: Setting up new push stream", "logFields", map[string]interface{}{
		"subscription": subName,
		"node":         nodeID,
	})
	// get subscription Model details
	subModel, err := subscriptionCore.Get(pushCtx, subName)
	if err != nil {
		logger.Ctx(pushCtx).Errorw("error fetching subscription", "error", err.Error())
		return nil, err
	}

	// set http connection timeout from the subscription
	if subModel.AckDeadlineSeconds != 0 {
		// make sure to convert sec to milli-sec
		// set the timeout value only if greater than the default
		ackDeadlineMs := int(subModel.AckDeadlineSeconds) * 1000
		if ackDeadlineMs > config.ConnectTimeoutMS {
			config.ConnectTimeoutMS = ackDeadlineMs
		}
		if ackDeadlineMs > config.ResponseHeaderTimeoutMS {
			config.ResponseHeaderTimeoutMS = ackDeadlineMs
		}
		logger.Ctx(ctx).Infow("worker: http timeouts set", "logFields", map[string]interface{}{
			"subscription":          subModel.Name,
			"topic":                 subModel.Topic,
			"connectTimeout":        config.ConnectTimeoutMS,
			"responseHeaderTimeout": config.ResponseHeaderTimeoutMS,
		})
	}

	doneCh := make(chan struct{})
	fanoutChan := make(chan *metrov1.ReceivedMessage, pullBatchSize)
	statusChan := make(chan deliveryStatus)
	httpclient := httpclient.NewClient(config)

	return &PushStream{
		ctx:              pushCtx,
		cancelFunc:       cancelFunc,
		nodeID:           nodeID,
		subscription:     subModel,
		subscriptionCore: subscriptionCore,
		subscriberCore:   subscriberCore,
		fanoutChan:       fanoutChan,
		statusChan:       statusChan,
		doneCh:           doneCh,
		httpClient:       httpclient,
		counter:          0,
		restartChan:      make(chan bool),
		stopChan:         make(chan bool),
	}, nil
}

func newPushEndpointRequest(message *metrov1.ReceivedMessage, subscription string) *metrov1.PushEndpointRequest {
	return &metrov1.PushEndpointRequest{
		Message:         message.Message,
		Subscription:    subscription,
		DeliveryAttempt: uint32(message.DeliveryAttempt),
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

// RunPushStreamManager return a runs a push stream manager used to handle restart/stop requests
func (ps *PushStream) RunPushStreamManager(ctx context.Context) {
	logger.Ctx(ctx).Infow("worker: started running stream manager", "subscription", ps.subscription.Name)

	go func() {
		defer logger.Ctx(ctx).Infow("worker: exiting stream manager", "subscription", ps.subscription.Name)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ps.GetRestartChannel():
				logger.Ctx(ctx).Infow("worker: restarting stream handler", "subscription", ps.subscription.Name)
				ps.Restart(ctx)
			case <-ps.GetStopChannel():
				logger.Ctx(ctx).Infow("worker: stopping stream handler", "subscription", ps.subscription.Name)
				err := ps.Stop()
				if err != nil {
					logger.Ctx(ctx).Infow("worker: error in exiting stream handler ", "error", err.Error())
				}
				return
			default:
				continue
			}
		}
	}()
}
