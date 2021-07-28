package subscriber

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/subscription"

	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
)

type DelayConsumer struct {
	interval subscription.Interval
	topic    string
	isPaused bool
	consumer messagebroker.Consumer
	bs       brokerstore.IBrokerStore
	handler  Handler
}

func NewDelayConsumer(ctx context.Context, topic string, bs brokerstore.IBrokerStore, handler Handler) (*DelayConsumer, error) {
	// only delay-consumer will consume from a subscription specific delay-topic, so can use the same groupID and groupInstanceID
	id := uuid.New().String()

	topicN := messagebroker.NormalizeTopicName(topic)
	consumer, err := bs.GetConsumer(ctx, messagebroker.ConsumerClientOptions{Topics: []string{topicN}, GroupID: id, GroupInstanceID: id})
	if err != nil {
		logger.Ctx(ctx).Errorw("delay-consumer: failed to create consumer", "error", err.Error())
		return nil, err
	}

	return &DelayConsumer{
		topic:    topic,
		consumer: consumer,
		bs:       bs,
		handler:  handler,
	}, nil
}

func (dc *DelayConsumer) Run(ctx context.Context) {
	logger.Ctx(ctx).Infow("delay-consumer: running", "topic", dc.topic)
	for {
		select {
		case <-ctx.Done():
			logger.Ctx(ctx).Infow("delay-consumer: stopping <-ctx.Done() called", "topic", dc.topic)
			return
		default:
			// read one message off the assigned delay topic
			resp, err := dc.consumer.ReceiveMessages(ctx, messagebroker.GetMessagesFromTopicRequest{NumOfMessages: 10, TimeoutMs: int(defaultBrokerOperationsTimeoutMs)})
			if messagebroker.IsErrorRecoverable(err) {
				logger.Ctx(ctx).Errorw("delay-consumer: got recoverable error in receiving messages, continuing", "topic", dc.topic, "error", err.Error())
				continue
			} else if err != nil {
				logger.Ctx(ctx).Errorw("delay-consumer: error in receiving messages", "topic", dc.topic, "error", err.Error())
				return
			}

			// no messages were found on the topic
			if len(resp.PartitionOffsetWithMessages) == 0 {
				continue
			}

			// ideally this should be only one message, that is the peek message off the topic
			for _, msg := range resp.PartitionOffsetWithMessages {
				// check if message can be processed
				if time.Now().Unix() > msg.NextDeliveryTime.Unix() {
					if dc.isPaused {
						logger.Ctx(ctx).Infow("delay-consumer: resuming", "topic", dc.topic)
						dc.consumer.Resume(ctx, messagebroker.ResumeOnTopicRequest{Topic: dc.topic})
						dc.isPaused = false
					}

					if msg.HasReachedRetryThreshold() {
						// push to dead-letter topic directly in such cases
						_, err := dc.bs.GetProducer(ctx, messagebroker.ProducerClientOptions{
							Topic:     msg.DeadLetterTopic,
							TimeoutMs: defaultBrokerOperationsTimeoutMs,
						})
						if err != nil {
							logger.Ctx(ctx).Errorw("delay-consumer: failed to push to dead-letter topic", "topic", msg.DeadLetterTopic, "error", err.Error())
							// do not commit and continue
							continue
						}
					} else {
						// else submit to handler for further processing
						err := dc.handler.Do(ctx, msg)
						if err != nil {
							logger.Ctx(ctx).Errorw("delay-consumer: error in msg handler", "topic", dc.topic, "error", err.Error())
							// do not commit and continue
							continue
						}
					}

					// commit the message
					_, err = dc.consumer.CommitByPartitionAndOffset(ctx, messagebroker.CommitOnTopicRequest{
						Topic:     msg.Topic,
						Partition: msg.Partition,
						Offset:    msg.Offset + 1,
					})

					if err != nil {
						logger.Ctx(ctx).Errorw("delay-consumer: error on commit", "topic", dc.topic, "error", err.Error())
						continue
					}

				} else {
					// pause the consumer, if not already paused
					if !dc.isPaused {
						logger.Ctx(ctx).Infow("delay-consumer: pausing", "topic", dc.topic)
						dc.consumer.Pause(ctx, messagebroker.PauseOnTopicRequest{Topic: dc.topic})
						dc.isPaused = true
					}
					// we can exit the loop if any one message is not in the allowed consumption time
					break
				}
			}
		}
	}
}
