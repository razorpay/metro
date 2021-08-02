package subscriber

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/razorpay/metro/pkg/logger"

	"github.com/razorpay/metro/pkg/messagebroker"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/subscription"
)

const (
	defaultBrokerOperationsTimeoutMs int64 = 100
)

// IRetrier ...
type IRetrier interface {
	Handle(ctx context.Context, msg messagebroker.ReceivedMessage) error
	Stop()
}

// Retrier ...
type Retrier struct {
	dc             *subscription.DelayConfig
	bs             brokerstore.IBrokerStore
	delayConsumers map[subscription.Interval]*DelayConsumer // TODO: use sync map here?
}

// NewRetrier ...
func NewRetrier(ctx context.Context, dc *subscription.DelayConfig, bs brokerstore.IBrokerStore, handler Handler) (IRetrier, error) {
	delayConsumers := make(map[subscription.Interval]*DelayConsumer, len(dc.GetDelayTopics()))

	for interval, config := range dc.GetDelayTopicsMap() {
		dc, err := NewDelayConsumer(ctx, config, bs, handler)
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

// Stop ...
func (r *Retrier) Stop() {
	wg := sync.WaitGroup{}
	for _, dc := range r.delayConsumers {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			dc.cancelFunc()
			// wait for delay-consumer shutdown to complete
			<-dc.doneCh
		}(&wg)
	}
	wg.Wait()
}

// Handle ...
func (r *Retrier) Handle(ctx context.Context, msg messagebroker.ReceivedMessage) error {

	logger.Ctx(ctx).Infow("retrier: received msg for retry", msg.LogFields()...)

	availableDelayIntervals := subscription.Intervals
	// EXPONENTIAL: nextDelayInterval + (delayIntervalMinutes * 2^(retryCount-1))
	nextDelayInterval := float64(msg.InitialDelayInterval) * math.Pow(2, float64(msg.CurrentRetryCount-1))

	// next allowed delay interval from the list of pre-defined intervals
	dInterval := findClosestDelayInterval(r.dc.MinimumBackoffInSeconds, r.dc.MaximumBackoffInSeconds, availableDelayIntervals, nextDelayInterval)
	dc := r.delayConsumers[dInterval]

	nextDeliveryTime := time.Now().Add(time.Duration(dInterval) * time.Second)

	// update message headers with new values
	newMessageHeaders := messagebroker.MessageHeader{
		MessageID:            msg.MessageID,
		SourceTopic:          msg.SourceTopic,
		Subscription:         msg.Subscription,
		CurrentRetryCount:    msg.CurrentRetryCount,
		MaxRetryCount:        msg.MaxRetryCount,
		CurrentTopic:         dc.config.Topic,
		NextDeliveryTime:     nextDeliveryTime,
		DeadLetterTopic:      msg.DeadLetterTopic,
		InitialDelayInterval: msg.InitialDelayInterval,
	}

	// new broker message
	newMessage := messagebroker.SendMessageToTopicRequest{
		Topic:         dc.config.Topic,
		Message:       msg.Data,
		MessageHeader: newMessageHeaders,
	}

	// given a message, produce to the correct topic
	producer, err := r.bs.GetProducer(ctx, messagebroker.ProducerClientOptions{
		Topic:     dc.config.Topic,
		TimeoutMs: defaultBrokerOperationsTimeoutMs,
	})
	if err != nil {
		logger.Ctx(ctx).Errorw("retrier: failed to produce to delay topic", "topic", dc.config.Topic)
		return err
	}

	_, err = producer.SendMessage(ctx, newMessage)
	if err != nil {
		return err
	}

	// update message headers for logging
	logger.Ctx(ctx).Infow("retrier: pushed msg from retry to handler", newMessage.LogFields()...)

	return nil
}

func findClosestDelayInterval(min uint, max uint, intervals []subscription.Interval, nextDelayInterval float64) subscription.Interval {
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
