package subscriber

import (
	"context"
	"time"

	"github.com/razorpay/metro/pkg/logger"
)

// NewWatcher is the subscriber watcher
type Watcher struct {
	subscriber *Subscriber
}

// NewWatcher returns subscriber watcher that monitors subscriber's health
func NewWatcher(subscriber *Subscriber) *Watcher {
	return &Watcher{subscriber: subscriber}
}

// Run loop
func (sw *Watcher) Run(ctx context.Context) {
	defer logger.Ctx(ctx).Infow("subscriberWatcher: exiting", "logFields", sw.subscriber.getLogFields())

	for {
		select {
		case <-ctx.Done():
			logger.Ctx(ctx).Infow("subscriberWatcher: <-ctx.Done() called", "logFields", sw.subscriber.getLogFields())
			return
		case <-time.NewTicker(10 * time.Second).C:
			timePassed := float64(-1)
			if !sw.subscriber.lastMessageProcessing.IsZero() {
				timePassed = time.Now().Sub(sw.subscriber.lastMessageProcessing).Seconds()
			}
			subscriberTimeSinceLastMsgProcessing.WithLabelValues(env, sw.subscriber.topic, sw.subscriber.subscription.Name).Observe(timePassed)
		}
	}
}
