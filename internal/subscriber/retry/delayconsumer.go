package retry

import (
	"context"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
)

// DelayConsumer ...
type DelayConsumer struct {
	subscriberID string
	ctx          context.Context
	cancelFunc   func()
	doneCh       chan struct{}
	subs         *subscription.Model
	topic        string
	isPaused     bool
	consumer     messagebroker.Consumer
	bs           brokerstore.IBrokerStore
	handler      MessageHandler
	// a paused consumer will not return new messages, so this cachedMsg will be used for lookups
	// till the needed time elapses
	cachedMsgs []messagebroker.ReceivedMessage
}

// NewDelayConsumer inits a new delay-consumer with the pre-defined message handler
func NewDelayConsumer(ctx context.Context, subscriberID string, topic string, subs *subscription.Model, bs brokerstore.IBrokerStore, handler MessageHandler) (*DelayConsumer, error) {

	delayCtx, cancel := context.WithCancel(ctx)
	// only delay-consumer will consume from a subscription specific delay-topic, so can use the same groupID and groupInstanceID
	consumerOps := messagebroker.ConsumerClientOptions{
		Topics:          []string{topic},
		GroupID:         subs.GetDelayConsumerGroupID(topic),
		GroupInstanceID: subs.GetDelayConsumerGroupInstanceID(subscriberID, topic),
	}
	consumer, err := bs.GetConsumer(ctx, consumerOps)
	if err != nil {
		logger.Ctx(ctx).Errorw("delay-consumer: failed to create consumer", "error", err.Error())
		return nil, err
	}

	// on init, make sure to call resume. This is done just to ensure any previously paused consumers get resumed on boot up.
	consumer.Resume(ctx, messagebroker.ResumeOnTopicRequest{Topic: topic})

	return &DelayConsumer{
		subscriberID: subscriberID,
		ctx:          delayCtx,
		cancelFunc:   cancel,
		topic:        topic,
		consumer:     consumer,
		subs:         subs,
		bs:           bs,
		handler:      handler,
		doneCh:       make(chan struct{}),
		cachedMsgs:   make([]messagebroker.ReceivedMessage, 0),
	}, nil
}

// Run spawns the delay-consumer
func (dc *DelayConsumer) Run(ctx context.Context) {
	defer close(dc.doneCh)

	logger.Ctx(ctx).Infow("delay-consumer: running", dc.LogFields()...)
	for {
		select {
		case <-dc.ctx.Done():
			logger.Ctx(dc.ctx).Infow("delay-consumer: stopping <-ctx.Done() called", dc.LogFields()...)
			dc.bs.RemoveConsumer(ctx, messagebroker.ConsumerClientOptions{GroupID: dc.subs.GetDelayConsumerGroupID(dc.topic), GroupInstanceID: dc.subs.GetDelayConsumerGroupInstanceID(dc.subscriberID, dc.topic)})
			dc.consumer.Close(dc.ctx)
			return
		default:
			resp, err := dc.consumer.ReceiveMessages(dc.ctx, messagebroker.GetMessagesFromTopicRequest{NumOfMessages: 10, TimeoutMs: int(defaultBrokerOperationsTimeoutMs)})
			if len(resp.PartitionOffsetWithMessages) > 0 {
				logger.Ctx(ctx).Infow("delay-consumer: non zero messages received", dc.LogFields("len", len(resp.PartitionOffsetWithMessages))...)
			}
			if err != nil {
				if !messagebroker.IsErrorRecoverable(err) {
					logger.Ctx(dc.ctx).Errorw("delay-consumer: error in receiving messages", dc.LogFields("error", err.Error())...)
					return
				}
			}

			for _, msg := range resp.PartitionOffsetWithMessages {
				dc.cachedMsgs = append(dc.cachedMsgs, msg)
			}

			dc.processMsgs()
		}
	}
}

// resumes a paused delay-consumer. Additionally process any previously cached messages in buffer.
func (dc *DelayConsumer) resume() {
	logger.Ctx(dc.ctx).Infow("delay-consumer: resuming", dc.LogFields("error", nil)...)
	dc.consumer.Resume(dc.ctx, messagebroker.ResumeOnTopicRequest{Topic: dc.topic})
	dc.isPaused = false
}

// paused an active delay-consumer. Additionally caches the last seen message locally.
func (dc *DelayConsumer) pause() {
	logger.Ctx(dc.ctx).Infow("delay-consumer: pausing", dc.LogFields()...)
	dc.consumer.Pause(dc.ctx, messagebroker.PauseOnTopicRequest{Topic: dc.topic})
	dc.isPaused = true
}

// push a message onto the configured dead letter topic
func (dc *DelayConsumer) pushToDeadLetter(msg *messagebroker.ReceivedMessage) error {
	logger.Ctx(dc.ctx).Infow("delay-consumer: pushing to dead-letter", dc.LogFields()...)
	dlProducer, err := dc.bs.GetProducer(dc.ctx, messagebroker.ProducerClientOptions{
		Topic:     msg.DeadLetterTopic,
		TimeoutMs: defaultBrokerOperationsTimeoutMs,
	})
	if err != nil {
		return err
	}

	_, err = dlProducer.SendMessage(dc.ctx, messagebroker.SendMessageToTopicRequest{
		Topic:         msg.DeadLetterTopic,
		Message:       msg.Data,
		TimeoutMs:     int(defaultBrokerOperationsTimeoutMs),
		MessageHeader: msg.MessageHeader,
	})
	if err != nil {
		return err
	}

	return nil
}

// processes a given message of the delay-topic. takes care of orchestrating the checks to determine pause,resume of the consumer,
// push to dead-letter topic in case the number of retries breaches allowed threshold.
func (dc *DelayConsumer) processMsgs() {
	for len(dc.cachedMsgs) > 0 {
		msg := dc.cachedMsgs[0]

		if msg.CanProcessMessage() {
			if msg.HasReachedRetryThreshold() {
				// push to dead-letter topic directly in such cases
				logger.Ctx(dc.ctx).Infow("delay-consumer: publishing to DLQ topic", dc.LogFields("messageID", msg.MessageID)...)
				err := dc.pushToDeadLetter(&msg)
				if err != nil {
					logger.Ctx(dc.ctx).Errorw("delay-consumer: failed to push to dead-letter topic",
						dc.LogFields("messageID", msg.MessageID, "topic", msg.DeadLetterTopic, "error", err.Error())...,
					)
					return
				}
			} else {
				// submit to
				logger.Ctx(dc.ctx).Infow("delay-consumer: processing message", dc.LogFields("messageID", msg.MessageID)...)
				err := dc.handler.Do(dc.ctx, msg)
				if err != nil {
					logger.Ctx(dc.ctx).Errorw("delay-consumer: error in msg handler",
						dc.LogFields("messageID", msg.MessageID, "error", err.Error())...)
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
				logger.Ctx(dc.ctx).Errorw("delay-consumer: error on commit", dc.LogFields("error", err.Error())...)
				return
			}

			// evict the processed message from cached messages
			dc.cachedMsgs = dc.cachedMsgs[1:]
		} else {
			// pause the consumer, if not already paused
			if !dc.isPaused {
				logger.Ctx(dc.ctx).Infow("delay-consumer: pausing consumer for message",
					dc.LogFields("messageID", msg.MessageID)...)
				dc.pause()
			}
			break
		}
	}
	if len(dc.cachedMsgs) == 0 && dc.isPaused {
		// pull messages from the broker to process the next batch of messages
		dc.resume()
	}

}

// LogFields ...
func (dc *DelayConsumer) LogFields(kv ...interface{}) []interface{} {
	fields := []interface{}{
		"delayConsumerConfig", map[string]interface{}{
			"topic":           dc.topic,
			"groupID":         dc.subs.GetDelayConsumerGroupID(dc.topic),
			"groupInstanceID": dc.subs.GetDelayConsumerGroupInstanceID(dc.subscriberID, dc.topic),
		},
	}

	fields = append(fields, kv...)
	return fields
}
