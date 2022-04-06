package retry

import (
	"context"
	"sync"
	"time"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/internal/topic"
	"github.com/razorpay/metro/pkg/cache"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
)

const (
	defaultBrokerOperationsTimeoutMs int64 = 100
)

// IRetrier interface over retrier core functionalities.
type IRetrier interface {
	Handle(context.Context, messagebroker.ReceivedMessage) error
	Start(context.Context) error
	Stop(context.Context)
}

// Retrier implements all business logic for IRetrier
type Retrier struct {
	subscriberID   string
	subs           *subscription.Model
	bs             brokerstore.IBrokerStore
	ch             cache.ICache
	backoff        Backoff
	finder         IntervalFinder
	handler        MessageHandler
	delayConsumers sync.Map
}

// Start starts a new retrier which internally takes care of spawning the needed delay-consumers.
func (r *Retrier) Start(ctx context.Context) error {
	// TODO : validate retrier params for nils and substitute with defaults
	for interval, topic := range r.subs.GetDelayTopicsMap() {
		dc, err := NewDelayConsumer(ctx, r.subscriberID, topic, r.subs, r.bs, r.handler, r.ch)
		if err != nil {
			return err
		}
		go dc.Run(ctx)                       // run the delay consumer
		r.delayConsumers.Store(interval, dc) // store the delay consumer for lookup
	}
	return nil
}

// Stop gracefully stop call the spawned delay-consumers for retry
func (r *Retrier) Stop(ctx context.Context) {
	logger.Ctx(ctx).Infow("retrier: stopping all delay consumers for topics", "delay_topics", r.subs.GetDelayTopics())
	wg := sync.WaitGroup{}
	r.delayConsumers.Range(func(_, dcFromMap interface{}) bool {
		wg.Add(1)
		dc := dcFromMap.(*DelayConsumer)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			dc.cancelFunc()
			// wait for delay-consumer shutdown to complete
			<-dc.doneCh
		}(&wg)
		return true
	})
	wg.Wait()
}

// Handle takes care of processing a given retry message.
func (r *Retrier) Handle(ctx context.Context, msg messagebroker.ReceivedMessage) error {

	logger.Ctx(ctx).Infow("retrier: received msg for retry", msg.LogFields()...)

	// calculate the next backoff using the strategy
	nextDelayInterval := r.backoff.Next(BackoffPolicy{
		startInterval: float64(msg.InitialDelayInterval),
		lastInterval:  float64(msg.CurrentDelayInterval),
		count:         float64(msg.CurrentRetryCount),
		exponential:   2,
	})

	// find next allowed delay interval from the list of pre-defined intervals
	dInterval := r.finder.Next(IntervalFinderParams{
		min:           r.subs.RetryPolicy.MinimumBackoff,
		max:           r.subs.RetryPolicy.MaximumBackoff,
		delayInterval: nextDelayInterval,
		intervals:     topic.Intervals,
	})

	dcFromMap, _ := r.delayConsumers.Load(dInterval)
	dc := dcFromMap.(*DelayConsumer)

	nextDeliveryTime := time.Now().Add(time.Duration(dInterval) * time.Second)

	logger.Ctx(ctx).Infow("retrier: resolved delay topic for retry", "dc.topic", dc.topic, "interval", dInterval, "nextInterval", nextDelayInterval)
	// update message headers with new values
	newMessageHeaders := messagebroker.MessageHeader{
		MessageID:            msg.MessageID,
		SourceTopic:          msg.SourceTopic,
		Subscription:         msg.Subscription,
		CurrentRetryCount:    msg.CurrentRetryCount,
		RetryTopic:           msg.RetryTopic,
		MaxRetryCount:        msg.MaxRetryCount,
		CurrentTopic:         dc.topic,
		NextDeliveryTime:     nextDeliveryTime,
		DeadLetterTopic:      msg.DeadLetterTopic,
		InitialDelayInterval: msg.InitialDelayInterval,
		CurrentDelayInterval: uint(nextDelayInterval),
		ClosestDelayInterval: uint(dInterval),
		CurrentSequence:      msg.CurrentSequence,
		PrevSequence:         msg.PrevSequence,
	}

	// new broker message
	newMessage := messagebroker.SendMessageToTopicRequest{
		Topic:         dc.topic,
		Message:       msg.Data,
		OrderingKey:   msg.OrderingKey,
		MessageHeader: newMessageHeaders,
	}

	// given a message, produce to the correct topic
	producer, err := r.bs.GetProducer(ctx, messagebroker.ProducerClientOptions{
		Topic:     dc.topic,
		TimeoutMs: defaultBrokerOperationsTimeoutMs,
	})
	if err != nil {
		logger.Ctx(ctx).Errorw("retrier: failed to init producer for delay topic", "topic", dc.topic, "error", err.Error())
		return err
	}

	// push message onto the identified delay-topic
	_, err = producer.SendMessage(ctx, newMessage)
	if err != nil {
		logger.Ctx(ctx).Errorw("retrier: failed to produce to delay topic", "topic", dc.topic, "error", err.Error())
		return err
	}

	logger.Ctx(ctx).Infow("retrier: pushed msg to delay topic", newMessage.LogFields()...)

	return nil
}

// helper function to calculate all the retry intervals
func findAllRetryIntervals(min, max, currentRetryCount, maxRetryCount, currentInterval int, availableDelayIntervals []topic.Interval) []float64 {
	expectedIntervals := make([]float64, 0)

	nef := NewExponentialWindowBackoff()
	finder := NewClosestIntervalWithCeil()

	for currentRetryCount <= maxRetryCount {
		nextDelayInterval := nef.Next(BackoffPolicy{
			startInterval: float64(min),
			lastInterval:  float64(currentInterval),
			count:         float64(currentRetryCount),
			exponential:   2,
		})

		closestInterval := finder.Next(IntervalFinderParams{
			min:           uint(min),
			max:           uint(max),
			delayInterval: nextDelayInterval,
			intervals:     availableDelayIntervals,
		})

		expectedIntervals = append(expectedIntervals, float64(closestInterval))
		currentInterval = int(closestInterval)
		currentRetryCount++
	}
	return expectedIntervals
}
