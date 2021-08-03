package subscriber

import (
	"context"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/subscription"

	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
)

// DelayConsumer ...
type DelayConsumer struct {
	ctx        context.Context
	cancelFunc func()
	doneCh     chan struct{}
	interval   subscription.Interval
	config     subscription.DelayConsumerConfig
	isPaused   bool
	consumer   messagebroker.Consumer
	bs         brokerstore.IBrokerStore
	handler    RetryMessageHandler
	// a paused consumer will not return new messages, so this cachedMsg will be used for lookups
	// till the needed time elapses
	cachedMsg *messagebroker.ReceivedMessage
}

// NewDelayConsumer inits a new delay-consumer with the pre-defined message handler
func NewDelayConsumer(ctx context.Context, config subscription.DelayConsumerConfig, bs brokerstore.IBrokerStore, handler RetryMessageHandler) (*DelayConsumer, error) {

	delayCtx, cancel := context.WithCancel(ctx)
	// only delay-consumer will consume from a subscription specific delay-topic, so can use the same groupID and groupInstanceID
	consumerOps := messagebroker.ConsumerClientOptions{Topics: []string{config.Topic}, GroupID: config.GroupID, GroupInstanceID: config.GroupInstanceID}
	consumer, err := bs.GetConsumer(ctx, consumerOps)
	if err != nil {
		logger.Ctx(ctx).Errorw("delay-consumer: failed to create consumer", "error", err.Error())
		return nil, err
	}

	// on init, make sure to call resume. This is done just to ensure any previously paused consumers get resumed on boot up.
	consumer.Resume(ctx, messagebroker.ResumeOnTopicRequest{Topic: config.Topic})

	return &DelayConsumer{
		ctx:        delayCtx,
		cancelFunc: cancel,
		config:     config,
		consumer:   consumer,
		bs:         bs,
		handler:    handler,
		doneCh:     make(chan struct{}),
	}, nil
}

// Run spawns the delay-consumer
func (dc *DelayConsumer) Run(ctx context.Context) {
	defer close(dc.doneCh)

	logger.Ctx(ctx).Infow("delay-consumer: running", dc.config.LogFields()...)
	for {
		select {
		case <-dc.ctx.Done():
			logger.Ctx(dc.ctx).Infow("delay-consumer: stopping <-ctx.Done() called", dc.config.LogFields()...)
			dc.bs.RemoveConsumer(ctx, dc.config.GroupInstanceID, messagebroker.ConsumerClientOptions{GroupID: dc.config.GroupID})
			dc.consumer.Close(dc.ctx)
			return
		default:
			if dc.cachedMsg != nil && dc.cachedMsg.CanProcessMessage() {
				dc.resume()
			}

			resp, err := dc.consumer.ReceiveMessages(dc.ctx, messagebroker.GetMessagesFromTopicRequest{NumOfMessages: 10, TimeoutMs: int(defaultBrokerOperationsTimeoutMs)})
			if err != nil {
				if !messagebroker.IsErrorRecoverable(err) {
					logger.Ctx(dc.ctx).Errorw("delay-consumer: error in receiving messages", dc.config.LogFields("error", err.Error())...)
					return
				}
			}

			for _, msg := range resp.PartitionOffsetWithMessages {
				dc.processMsg(msg)
			}
		}
	}
}

// resumes a paused delay-consumer. Additionally process any previously cached messages in buffer.
func (dc *DelayConsumer) resume() {
	logger.Ctx(dc.ctx).Infow("delay-consumer: resuming", dc.config.LogFields("error", nil)...)
	dc.consumer.Resume(dc.ctx, messagebroker.ResumeOnTopicRequest{Topic: dc.config.Topic})
	dc.isPaused = false

	// process the cached message as well
	dc.processMsg(*dc.cachedMsg)
	dc.cachedMsg = nil
}

// paused an active delay-consumer. Additionally caches the last seen message locally.
func (dc *DelayConsumer) pause(msg *messagebroker.ReceivedMessage) {
	logger.Ctx(dc.ctx).Infow("delay-consumer: pausing", dc.config.LogFields()...)
	dc.consumer.Pause(dc.ctx, messagebroker.PauseOnTopicRequest{Topic: dc.config.Topic})
	dc.isPaused = true
	dc.cachedMsg = msg
}

// processes a given message of the delay-topic. takes care of orchestrating the checks to determine pause,resume of the consumer,
// push to dead-letter topic in case the number of retries breaches allowed threshold.
func (dc *DelayConsumer) processMsg(msg messagebroker.ReceivedMessage) {
	if msg.CanProcessMessage() {
		if dc.isPaused {
			dc.resume()
		}

		if msg.HasReachedRetryThreshold() {
			// push to dead-letter topic directly in such cases
			_, err := dc.bs.GetProducer(dc.ctx, messagebroker.ProducerClientOptions{
				Topic:     msg.DeadLetterTopic,
				TimeoutMs: defaultBrokerOperationsTimeoutMs,
			})
			if err != nil {
				logger.Ctx(dc.ctx).Errorw("delay-consumer: failed to push to dead-letter topic", "topic", msg.DeadLetterTopic, "error", err.Error())
				return
			}
		} else {
			// submit to handler
			err := dc.handler.Do(dc.ctx, msg)
			if err != nil {
				logger.Ctx(dc.ctx).Errorw("delay-consumer: error in msg handler", dc.config.LogFields("error", err.Error())...)
				return
			}
		}

		// commit the message
		_, err := dc.consumer.CommitByPartitionAndOffset(dc.ctx, messagebroker.CommitOnTopicRequest{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset + 1,
		})

		if err != nil {
			logger.Ctx(dc.ctx).Errorw("delay-consumer: error on commit", dc.config.LogFields("error", err.Error())...)
			return
		}

	} else {
		// pause the consumer, if not already paused
		if !dc.isPaused {
			dc.pause(&msg)
		}
	}
}
