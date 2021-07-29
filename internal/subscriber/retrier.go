package subscriber

import (
	"context"
	"math"
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

func (r *Retrier) Handle(ctx context.Context, msg messagebroker.ReceivedMessage) error {

	availableDelayIntervals := subscription.Intervals // 5,10,15
	// EXPONENTIAL: nextDelayInterval + (delayIntervalMinutes * 2^(retryCount-1))
	nextDelayInterval := float64(msg.InitialDelayInterval) + math.Pow(2, float64(msg.CurrentRetryCount-1))

	// next allowed delay interval from the list of pre-defined intervals
	dInterval := findCloseDelayInterval(r.dc.MinimumBackoffInSeconds, r.dc.MaximumBackoffInSeconds, availableDelayIntervals, nextDelayInterval)
	dc := r.delayConsumers[dInterval]

	nextDeliveryTime := time.Now().Add(time.Duration(dInterval) * time.Second)

	// given a message, produce to the correct topic
	producer, err := r.bs.GetProducer(ctx, messagebroker.ProducerClientOptions{
		Topic:     dc.topic,
		TimeoutMs: defaultBrokerOperationsTimeoutMs,
	})
	if err != nil {
		return err
	}

	_, err = producer.SendMessage(ctx, messagebroker.SendMessageToTopicRequest{
		Topic:   dc.topic,
		Message: msg.Data,
		MessageHeader: messagebroker.MessageHeader{
			MessageID:            msg.MessageID,
			SourceTopic:          msg.SourceTopic,
			Subscription:         msg.Subscription,
			CurrentRetryCount:    msg.CurrentRetryCount,
			MaxRetryCount:        msg.MaxRetryCount,
			CurrentTopic:         dc.topic,
			NextDeliveryTime:     nextDeliveryTime,
			DeadLetterTopic:      msg.DeadLetterTopic,
			InitialDelayInterval: msg.InitialDelayInterval,
		},
	})
	if err != nil {
		return err
	}

	return nil
}

func findCloseDelayInterval(min uint, max uint, intervals []subscription.Interval, nextDelayInterval float64) subscription.Interval {
	newDelay := nextDelayInterval
	if newDelay < float64(min) {
		newDelay = float64(min)
	} else if newDelay > float64(max) {
		return subscription.Delay600sec
	}

	// find the closest interval greater than newDelay
	for _, interval := range intervals {
		if float64(interval) > newDelay {
			return interval
		}
	}

	// by default use the max available delay
	return subscription.Delay600sec
}
