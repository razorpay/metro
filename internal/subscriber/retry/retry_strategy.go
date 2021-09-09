package retry

import (
	"context"
	"math"

	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/logger"
)

var invalidDelayType = merror.New(merror.Internal, "delay type is invalid")

type delayType string

const (
	exponentialDelayType delayType = "EXPONENTIAL"
)

// iDelayCalculator - An interface to implement various retry strategies
type iDelayCalculator interface {
	// CalculateNextDelayInterval - Given the min, max and current interval,
	// compute the next delay interval for the given retry count
	// intervals are in seconds
	CalculateNextDelayInterval(ctx context.Context, minInterval, maxInterval, currentInterval subscription.Interval, delayBuckets []subscription.Interval, retryCount uint) (interval subscription.Interval, e error)
}

type exponentialDelayCalculator struct{}

// Using below formula
// EXPONENTIAL: nextDelayInterval = currentInterval + (minInterval * 2^(retryCount-1))
// Refer http://exponentialbackoffcalculator.com/
func (e *exponentialDelayCalculator) CalculateNextDelayInterval(ctx context.Context, minInterval, maxInterval, currentInterval subscription.Interval,
	delayBuckets []subscription.Interval, retryCount uint) (subscription.Interval, error) {
	delay := float64(currentInterval) + float64(minInterval)*math.Pow(2, float64(retryCount)-1)
	delay = math.Min(delay, float64(maxInterval))
	return subscription.Interval(delay), nil
}

func getDelayCalculator(ctx context.Context, dType delayType) (iDelayCalculator, error) {
	switch dType {
	case exponentialDelayType:
		return &exponentialDelayCalculator{}, nil
	default:
		logger.Ctx(ctx).Errorw("retrier: invalid delay type", "delay type", dType, "error", invalidDelayType.Error())
		return nil, invalidDelayType
	}
}
