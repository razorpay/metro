package subscriber

import (
	"context"
	"time"

	"github.com/razorpay/metro/pkg/messagebroker"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/subscription"
)

const (
	defaultBrokerOperationsTimeoutMs int64 = 1000
)

type IRetrier interface {
	Handle(ctx context.Context, msg messagebroker.ReceivedMessage) error
}

type Retrier struct {
	dc             *subscription.DelayConfig
	bs             brokerstore.IBrokerStore
	delayConsumers map[subscription.Interval]*DelayConsumer // TODO: use sync map here?
}

func NewRetrier(ctx context.Context, dc *subscription.DelayConfig, bs brokerstore.IBrokerStore, handler Handler) (IRetrier, error) {
	delayConsumers := make(map[subscription.Interval]*DelayConsumer, len(dc.GetDelayTopics()))
	for interval, topic := range dc.GetDelayTopicsMap() {
		dc, err := NewDelayConsumer(ctx, topic, bs, handler)
		if err != nil {
			return nil, err
		}
		go dc.Run(ctx)                // run the delay consumer
		delayConsumers[interval] = dc // store the delay consumer for lookup
	}

	r := &Retrier{
		dc:             dc,
		bs:             bs,
		delayConsumers: delayConsumers,
	}

	return r, nil
}

func (r *Retrier) findClosestDelayConsumer(msg messagebroker.ReceivedMessage) *DelayConsumer {
	// logic to leverage the delay config and current delay to find the next delay topic
	return &DelayConsumer{}
}

func (r *Retrier) Handle(ctx context.Context, msg messagebroker.ReceivedMessage) error {

	dc := r.findClosestDelayConsumer(msg)
	msg.NextDeliveryTime = time.Now() // int32(msg.PublishTime) + int32(dc.interval)
	msg.NextTopic = dc.topic

	// given a message, produce to the correct topic
	producer, err := r.bs.GetProducer(ctx, messagebroker.ProducerClientOptions{
		Topic:     dc.topic,
		TimeoutMs: defaultBrokerOperationsTimeoutMs,
	})
	if err != nil {
		return err
	}

	newMsg := prepareNextMessage(r.dc, msg)

	_, err = producer.SendMessage(ctx, messagebroker.SendMessageToTopicRequest{
		Topic:   newMsg.NextTopic,
		Message: newMsg.Data,
		MessageHeader: messagebroker.MessageHeader{
			MessageID:         msg.MessageID,
			SourceTopic:       msg.SourceTopic,
			Subscription:      msg.Subscription,
			CurrentRetryCount: msg.CurrentRetryCount + 1,
			MaxRetryCount:     msg.MaxRetryCount,
			NextTopic:         "",
			NextDeliveryTime:  time.Time{},
		},
	})
	if err != nil {
		return err
	}

	return nil
}

func prepareNextMessage(dc *subscription.DelayConfig, msg messagebroker.ReceivedMessage) messagebroker.ReceivedMessage {
	// do all the needed calculations here
	return messagebroker.ReceivedMessage{}
}
