package subscriber

import (
	"context"
	"time"

	"github.com/opentracing/opentracing-go"

	"github.com/razorpay/metro/internal/subscriber/retry"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

const (
	deadlineTickerInterval = 200 * time.Millisecond
	minAckDeadline         = 10 * time.Minute
)

// ISubscriber is interface over high level subscriber
type ISubscriber interface {
	GetID() string
	GetSubscriptionName() string
	GetResponseChannel() chan *metrov1.PullResponse
	GetRequestChannel() chan *PullRequest
	GetAckChannel() chan *AckMessage
	GetModAckChannel() chan *ModAckMessage
	GetErrorChannel() chan error
	Stop()
	Run(ctx context.Context)
}

// Implementation is an interface abstracting different types of subscribers
type Implementation interface {
	GetSubscription() *subscription.Model
	GetSubscriberID() string

	Pull(ctx context.Context, req *PullRequest, responseChan chan *metrov1.PullResponse, errChan chan error)
	Acknowledge(ctx context.Context, req *AckMessage, errChan chan error)
	ModAckDeadline(ctx context.Context, req *ModAckMessage, errChan chan error)
	EvictUnackedMessagesPastDeadline(ctx context.Context, errChan chan error)

	CanConsumeMore() bool
}

// Subscriber consumes messages from a topic
type Subscriber struct {
	subscription   *subscription.Model
	topic          string
	subscriberID   string
	requestChan    chan *PullRequest
	responseChan   chan *metrov1.PullResponse
	ackChan        chan *AckMessage
	modAckChan     chan *ModAckMessage
	deadlineTicker *time.Ticker
	errChan        chan error
	closeChan      chan struct{}
	consumer       IConsumer // consume messages from primary topic and retry topic
	cancelFunc     func()
	ctx            context.Context
	retrier        retry.IRetrier
	subscriberImpl Implementation
}

// GetID ...
func (s *Subscriber) GetID() string {
	return s.subscriberID
}

// GetSubscriptionName ...
func (s *Subscriber) GetSubscriptionName() string {
	return s.subscription.Name
}

// GetRequestChannel returns the chan from where request is received
func (s *Subscriber) GetRequestChannel() chan *PullRequest {
	return s.requestChan
}

// GetResponseChannel returns the chan where response is written
func (s *Subscriber) GetResponseChannel() chan *metrov1.PullResponse {
	return s.responseChan
}

// GetErrorChannel returns the channel where error is written
func (s *Subscriber) GetErrorChannel() chan error {
	return s.errChan
}

// GetAckChannel returns the chan from where ack is received
func (s *Subscriber) GetAckChannel() chan *AckMessage {
	return s.ackChan
}

// GetModAckChannel returns the chan where mod ack is written
func (s *Subscriber) GetModAckChannel() chan *ModAckMessage {
	return s.modAckChan
}

// Run loop
func (s *Subscriber) Run(ctx context.Context) {
	logger.Ctx(ctx).Infow("subscriber: started running subscriber", "logFields", s.getLogFields())
	for {
		select {
		case req := <-s.requestChan:
			// on channel closure, we can get nil data
			if req == nil || ctx.Err() != nil {
				continue
			}
			caseStartTime := time.Now()
			s.pull(req)
			subscriberTimeTakenInRequestChannelCase.WithLabelValues(env, s.topic, s.subscription.Name).Observe(time.Now().Sub(caseStartTime).Seconds())
		case ackRequest := <-s.ackChan:
			caseStartTime := time.Now()
			if ackRequest == nil || ctx.Err() != nil {
				continue
			}
			s.acknowledge(ackRequest)
			subscriberTimeTakenInAckChannelCase.WithLabelValues(env, s.topic, s.subscription.Name).Observe(time.Now().Sub(caseStartTime).Seconds())
		case modAckRequest := <-s.modAckChan:
			caseStartTime := time.Now()
			if modAckRequest == nil || ctx.Err() != nil {
				continue
			}
			s.modifyAckDeadline(modAckRequest)
			subscriberTimeTakenInModAckChannelCase.WithLabelValues(env, s.topic, s.subscription.Name).Observe(time.Now().Sub(caseStartTime).Seconds())
		case <-s.deadlineTicker.C:
			caseStartTime := time.Now()
			if ctx.Err() != nil {
				continue
			}
			s.subscriberImpl.EvictUnackedMessagesPastDeadline(ctx, s.GetErrorChannel())
			subscriberTimeTakenInDeadlineChannelCase.WithLabelValues(env, s.topic, s.subscription.Name).Observe(time.Now().Sub(caseStartTime).Seconds())
		case err := <-s.errChan:
			if ctx.Err() != nil {
				continue
			}
			if err != nil {
				logger.Ctx(ctx).Errorw("subscriber: got error on errCh channel", "logFields", s.getLogFields(), "error", err.Error())
			}
		case <-ctx.Done():
			logger.Ctx(ctx).Infow("subscriber: <-ctx.Done() called", "logFields", s.getLogFields())

			s.deadlineTicker.Stop()

			// close the response channel to stop any new message processing
			close(s.responseChan)
			close(s.errChan)

			s.consumer.Close(ctx)
			close(s.closeChan)
			return
		}
	}
}

// Stop the subscriber
func (s *Subscriber) Stop() {

	// gracefully shutdown the retrier delay consumers
	s.retrier.Stop(s.ctx)

	s.cancelFunc()

	<-s.closeChan
}

func retryMessage(ctx context.Context, s Implementation, consumer IConsumer, retrier retry.IRetrier, msg messagebroker.ReceivedMessage) error {
	// for older subscriptions, delayConfig will not get auto-created
	if retrier == nil {
		logger.Ctx(ctx).Infow("subscriber: skipping retry as retrier not configured", "logFields", getLogFields(s))
		return nil
	}

	subscription := s.GetSubscription()
	// prepare message headers to be used by retrier
	msg.SourceTopic = subscription.Topic
	msg.RetryTopic = subscription.GetRetryTopic() // push retried messages to primary retry topic
	msg.CurrentTopic = subscription.Topic         // initially these will be same
	msg.Subscription = subscription.Name
	msg.CurrentRetryCount = msg.CurrentRetryCount + 1 // should be zero to begin with
	msg.MaxRetryCount = subscription.DeadLetterPolicy.MaxDeliveryAttempts
	msg.DeadLetterTopic = subscription.DeadLetterPolicy.DeadLetterTopic
	msg.InitialDelayInterval = subscription.RetryPolicy.MinimumBackoff

	err := retrier.Handle(ctx, msg)
	if err != nil {
		logger.Ctx(ctx).Errorw("subscriber: push to retrier failed", "logFields", getLogFields(s), "error", err.Error())
		return err
	}

	return nil
}

func retryAndCommitMessage(ctx context.Context, s Implementation, consumer IConsumer, retrier retry.IRetrier, msg messagebroker.ReceivedMessage, errChan chan error) {

	err := retryMessage(ctx, s, consumer, retrier, msg)
	if err != nil {
		errChan <- err
		return
	}

	// commit on the primary topic after message has been submitted for retry
	_, err = consumer.CommitByPartitionAndOffset(ctx, messagebroker.CommitOnTopicRequest{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		// add 1 to current offset
		// https://docs.confluent.io/5.5.0/clients/confluent-kafka-go/index.html#pkg-overview
		Offset: msg.Offset + 1,
	})
	if err != nil {
		logger.Ctx(ctx).Errorw("subscriber: failed to commit message", "logFields", getLogFields(s), "error", err.Error())
		errChan <- err
		return
	}
}

func (s *Subscriber) pull(req *PullRequest) {
	span, ctx := opentracing.StartSpanFromContext(req.ctx, "Subscriber:Pull", opentracing.Tags{
		"subscriber":   s.subscriberID,
		"subscription": s.subscription.Name,
		"topic":        s.subscription.Topic,
	})
	defer span.Finish()
	s.subscriberImpl.Pull(ctx, req, s.GetResponseChannel(), s.GetErrorChannel())
}

func (s *Subscriber) acknowledge(req *AckMessage) {
	span, ctx := opentracing.StartSpanFromContext(req.ctx, "Subscriber:Ack", opentracing.Tags{
		"subscriber":   req.SubscriberID,
		"topic":        req.Topic,
		"subscription": s.subscription.Name,
		"message_id":   req.MessageID,
		"partition":    req.Partition,
	})
	defer span.Finish()
	s.subscriberImpl.Acknowledge(ctx, req, s.GetErrorChannel())
}

func (s *Subscriber) modifyAckDeadline(req *ModAckMessage) {
	span, ctx := opentracing.StartSpanFromContext(req.ctx, "Subscriber:ModifyAck", opentracing.Tags{
		"subscriber":   req.AckMessage.SubscriberID,
		"topic":        req.AckMessage.Topic,
		"subscription": s.subscription.Name,
		"message_id":   req.AckMessage.MessageID,
		"partition":    req.AckMessage.Partition,
	})
	defer span.Finish()
	s.subscriberImpl.ModAckDeadline(ctx, req, s.GetErrorChannel())
}

func notifyPullMessageError(ctx context.Context, s Implementation, err error, responseChan chan *metrov1.PullResponse, errChan chan error) {
	logger.Ctx(ctx).Errorw("subscriber: error in receiving messages", "logFields", getLogFields(s), "error", err.Error())

	// Write empty data on the response channel in case of error, this is needed because sender blocks
	// on the response channel in a goroutine after sending request, error channel is not read until
	// response channel blocking call returns
	responseChan <- &metrov1.PullResponse{ReceivedMessages: make([]*metrov1.ReceivedMessage, 0)}

	// send error details via error channel
	errChan <- err
	return
}

func receiveMessages(ctx context.Context, s Implementation, consumer IConsumer, req *PullRequest) ([]messagebroker.ReceivedMessage, error) {
	// wrapping this code block in an anonymous function so that defer on time-taken metric can be scoped
	if s.CanConsumeMore() == false {
		logger.Ctx(ctx).Infow("subscriber: cannot consume more messages before acking", "logFields", getLogFields(s))
		// check if consumer is paused once maxOutstanding messages limit is hit
		if consumer.IsPaused(ctx) == false {
			consumer.PauseConsumer(ctx)
		}
	} else {
		// resume consumer if paused and is allowed to consume more messages
		if consumer.IsPaused(ctx) {
			consumer.ResumeConsumer(ctx)
		}
	}
	resp, err := consumer.ReceiveMessages(ctx, req.MaxNumOfMessages)
	if err != nil {
		return nil, err
	}
	return resp.Messages, nil
}

func getLogFields(s Implementation) map[string]interface{} {
	sub := s.GetSubscription()
	return map[string]interface{}{
		"topic":        sub.Topic,
		"subscription": sub.Name,
		"subscriberId": s.GetSubscriberID(),
	}
}

// returns a map of common fields to be logged
func (s *Subscriber) getLogFields() map[string]interface{} {
	return map[string]interface{}{
		"topic":        s.topic,
		"subscription": s.subscription.Name,
		"subscriberId": s.subscriberID,
	}
}
