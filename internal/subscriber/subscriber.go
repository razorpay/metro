package subscriber

import (
	"context"
	"strconv"
	"time"

	"github.com/opentracing/opentracing-go"

	"github.com/razorpay/metro/internal/subscriber/retry"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

const (
	deadlineTickerInterval      = 200 * time.Millisecond
	minAckDeadline              = 10 * time.Minute
	healthMonitorTickerInterval = 1 * time.Minute
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
	GetConsumedMessagesStats() map[string]interface{}

	Pull(ctx context.Context, req *PullRequest, responseChan chan *metrov1.PullResponse, errChan chan error)
	Acknowledge(ctx context.Context, req *AckMessage, errChan chan error)
	ModAckDeadline(ctx context.Context, req *ModAckMessage, errChan chan error)
	EvictUnackedMessagesPastDeadline(ctx context.Context, errChan chan error)

	CanConsumeMore() bool
}

// Subscriber consumes messages from a topic
type Subscriber struct {
	subscription        *subscription.Model
	topic               string
	subscriberID        string
	requestChan         chan *PullRequest
	responseChan        chan *metrov1.PullResponse
	ackChan             chan *AckMessage
	modAckChan          chan *ModAckMessage
	deadlineTicker      *time.Ticker
	healthMonitorTicker *time.Ticker
	errChan             chan error
	closeChan           chan struct{}
	consumer            IConsumer // consume messages from primary topic and retry topic
	cancelFunc          func()
	ctx                 context.Context
	retrier             retry.IRetrier
	subscriberImpl      Implementation
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
			logger.Ctx(ctx).Infow("subscriber: received mod ack for msg", "modAckReq", modAckRequest, "subscription", s.subscription.Name)
			s.modifyAckDeadline(modAckRequest)
			subscriberTimeTakenInModAckChannelCase.WithLabelValues(env, s.topic, s.subscription.Name).Observe(time.Now().Sub(caseStartTime).Seconds())
		case <-s.deadlineTicker.C:
			caseStartTime := time.Now()
			if ctx.Err() != nil {
				continue
			}
			s.subscriberImpl.EvictUnackedMessagesPastDeadline(ctx, s.GetErrorChannel())
			subscriberTimeTakenInDeadlineChannelCase.WithLabelValues(env, s.topic, s.subscription.Name).Observe(time.Now().Sub(caseStartTime).Seconds())
		case <-s.healthMonitorTicker.C:
			logger.Ctx(ctx).Infow("subscriber: heath check monitoring logs",
				"logFields", s.getLogFields(),
				"consumerPaused", s.consumer.IsPaused(ctx),
				"isPrimaryConsumerPaused", s.consumer.IsPrimaryPaused(ctx),
				"stats", s.subscriberImpl.GetConsumedMessagesStats(),
			)
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
	logger.Ctx(ctx).Infow("subscriber: send to retry", msg.LogFields()...)

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

	logger.Ctx(ctx).Infow("subscriber: retry params", "current topic", msg.CurrentTopic, "retrycount", msg.CurrentRetryCount, "retry topic", msg.RetryTopic)
	err := retrier.Handle(ctx, msg)
	if err != nil {
		logger.Ctx(ctx).Errorw("subscriber: push to retrier failed", "logFields", getLogFields(s), "error", err.Error())
		return err
	}

	return nil
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
	subscriberLastMsgProcessingTime.WithLabelValues(env, s.topic, s.subscription.Name, strconv.Itoa(int(req.Partition))).SetToCurrentTime()
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
	subscriberLastMsgProcessingTime.WithLabelValues(env, s.topic, s.subscription.Name, strconv.Itoa(int(req.AckMessage.Partition))).SetToCurrentTime()
}

func filterMessages(ctx context.Context, s Implementation, messages []*metrov1.ReceivedMessage, errChan chan error) []*metrov1.ReceivedMessage {
	fm := make([]*metrov1.ReceivedMessage, 0)
	for _, msg := range messages {
		// check if the subscription has any filter applied and check if the message satisfies it if any
		if checkFilterCriteria(ctx, s, msg) {
			fm = append(fm, msg)
		} else {
			// self acknowledging the message as it does not need to be delivered for this subscription
			//
			// Using subscriber's Acknowledge func instead of directly acking to consumer because of the following reason:
			// Let there be 3 messages - Msg-1, Msg-2 and Msg-3. Msg-1 and Msg-3 satisfies the filter criteria while Msg-3 doesn't.
			// If we directly ack Msg-2 using brokerStore consumer, in Kafka, new committed offset will be set to 2.
			// Now if for some reason, delivery of Msg-1 fails, we will not be able to retry it as the broker's committed offset is already set to 2.
			// To avoid this, subscriber Acknowledge is used which will wait for Msg-1 status before committing the offsets.
			logger.Ctx(ctx).Infow("subscriber: message filtered out", "logFields", getLogFields(s), "messageID", msg.Message.MessageId)
			ackMsg, _ := ParseAckID(msg.AckId)
			s.Acknowledge(ctx, ackMsg, errChan)
		}
	}
	return fm
}

// checks if the message satisfies filter criteria(if any) for the subscription
func checkFilterCriteria(ctx context.Context, s Implementation, msg *metrov1.ReceivedMessage) bool {
	sub := s.GetSubscription()
	if sub.FilterExpression != "" {
		subFilter, err := s.GetSubscription().GetFilterExpressionAsStruct()

		if err != nil {
			logger.Ctx(ctx).Errorw("subscriber: error in getting filter expression as a struct", "filter expression", sub.FilterExpression,
				"logfields", getLogFields(s), "error", err.Error())
		} else {
			res, err := subFilter.Evaluate(msg.Message.Attributes)
			if err != nil {
				logger.Ctx(ctx).Errorw("subscriber: error occurred during filter evaluation", "filter expression", sub.FilterExpression,
					"logfields", getLogFields(s), "error", err.Error())
			} else {
				if !res {
					logger.Ctx(ctx).Infow("subscriber: Message didn't satisfy the filter criteria", "messageID", msg.Message.MessageId, "filter expression", sub.FilterExpression,
						"logfields", getLogFields(s))
					return false
				}
			}
		}
	}
	return true
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
		logger.Ctx(ctx).Infow("subscriber: cannot consume more messages before acking",
			"logFields", getLogFields(s),
			"stats", s.GetConsumedMessagesStats(),
		)
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
