package subscription

import (
	"fmt"

	"github.com/pkg/errors"
)

// subs-delay-30-seconds, subs-delay-60-seconds ... subs-delay-600-seconds
const delayTopicNameFormat = "%v.delay.%v.seconds"

// projects/p1/topics/subs.delay.30.seconds
const delayTopicWithProjectNameFormat = "projects/%v/topics/%v.delay.%v.seconds"

// subs.delay.30.seconds-cg
const delayConsumerGroupIDFormat = "%v-cg"

// subs.delay.30.seconds-cgi
const delayConsumerGroupInstanceIDFormat = "%v-cgi"

const (
	defaultMinimumBackoffInSeconds    uint = 10
	lowerBoundMinimumBackoffInSeconds uint = 0
	upperBoundMinimumBackoffInSeconds uint = 600

	defaultMaximumBackoffInSeconds    uint = 600
	lowerBoundMaximumBackoffInSeconds uint = 0
	upperBoundMaximumBackoffInSeconds uint = 3600

	defaultMaxDeliveryAttempts    int32 = 5
	lowerBoundMaxDeliveryAttempts int32 = 1
	upperBoundMaxDeliveryAttempts int32 = 100
)

var (
	// ErrInvalidMinBackoff ...
	ErrInvalidMinBackoff = errors.New(fmt.Sprintf("min backoff should be between %v and %v seconds", lowerBoundMinimumBackoffInSeconds, upperBoundMinimumBackoffInSeconds))
	// ErrInvalidMaxBackoff ...
	ErrInvalidMaxBackoff = errors.New(fmt.Sprintf("max backoff should be between %v and %v seconds", lowerBoundMaximumBackoffInSeconds, upperBoundMaximumBackoffInSeconds))
	// ErrInvalidMinAndMaxBackoff ...
	ErrInvalidMinAndMaxBackoff = errors.New("min backoff should be less or equal to max backoff")
	// ErrInvalidMaxDeliveryAttempt ...
	ErrInvalidMaxDeliveryAttempt = errors.New(fmt.Sprintf("max delivery attempt should be between %v and %v", lowerBoundMaxDeliveryAttempts, upperBoundMaxDeliveryAttempts))
)

// Interval is internal delay type per allowed interval
type Interval uint

var (
	// Delay5sec 5sec
	Delay5sec Interval = 5
	// Delay30sec 30sec
	Delay30sec Interval = 30
	// Delay60sec 1min
	Delay60sec Interval = 60
	// Delay150sec 2.5min
	Delay150sec Interval = 150
	// Delay300sec 5min
	Delay300sec Interval = 300
	// Delay600sec 10min
	Delay600sec Interval = 600
	// Delay1800sec 30min
	Delay1800sec Interval = 1800
	// Delay3600sec 60min
	Delay3600sec Interval = 3600
)

var (
	// MinDelay ...
	MinDelay = Delay5sec
	// MaxDelay ...
	MaxDelay = Delay3600sec
)

// Intervals during subscription creation, query from the allowed intervals list, and create all the needed topics for retry.
var Intervals = []Interval{Delay5sec, Delay30sec, Delay60sec, Delay150sec, Delay300sec, Delay600sec, Delay1800sec, Delay3600sec}

// validateDelayConfig validates the given retry and dead-letter values for a subscription model
func validateDelayConfig(model *Model) error {

	minB := defaultMinimumBackoffInSeconds
	maxB := defaultMaximumBackoffInSeconds
	maxAtt := defaultMaxDeliveryAttempts

	if model.RetryPolicy != nil {
		minB = model.RetryPolicy.MinimumBackoff
		if minB < lowerBoundMinimumBackoffInSeconds || minB > upperBoundMinimumBackoffInSeconds {
			return ErrInvalidMinBackoff
		}
	}

	if model.RetryPolicy != nil {
		maxB = model.RetryPolicy.MaximumBackoff
		if maxB < lowerBoundMaximumBackoffInSeconds || maxB > upperBoundMaximumBackoffInSeconds {
			return ErrInvalidMaxBackoff
		}
	}

	if minB > maxB {
		return ErrInvalidMinAndMaxBackoff
	}

	// currently dl-topics are auto-created for subscriptions, refer GetValidatedModelForCreate()
	if model.DeadLetterPolicy != nil {
		maxAtt = model.DeadLetterPolicy.MaxDeliveryAttempts
		if maxAtt == 0 {
			// since dl-topics are auto-created, a user may never provide a dead-letter policy during subscription creation.
			// in such cases we set MaxDeliveryAttempts to a default value
			maxAtt = defaultMaxDeliveryAttempts
		} else if maxAtt < lowerBoundMaxDeliveryAttempts || maxAtt > upperBoundMaxDeliveryAttempts {
			return ErrInvalidMaxDeliveryAttempt
		}
	}

	// override model with correct values
	model.RetryPolicy = &RetryPolicy{
		MinimumBackoff: minB,
		MaximumBackoff: maxB,
	}

	model.DeadLetterPolicy = &DeadLetterPolicy{
		DeadLetterTopic:     model.GetDeadLetterTopic(),
		MaxDeliveryAttempts: maxAtt,
	}

	return nil
}
