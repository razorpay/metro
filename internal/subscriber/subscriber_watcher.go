package subscriber

import (
	"context"
	"time"

	"github.com/razorpay/metro/pkg/logger"
)

type SubscriberWatcher struct {
	subscriber *Subscriber
}

func NewSubscriberWatcher(subscriber *Subscriber) *SubscriberWatcher {
	return &SubscriberWatcher{subscriber: subscriber}
}

func (sw *SubscriberWatcher) Run(ctx context.Context) {
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
