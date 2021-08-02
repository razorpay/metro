package subscription

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/razorpay/metro/internal/topic"

	"github.com/pkg/errors"
)

// subs-delay-30-seconds, subs-delay-60-seconds ... subs-delay-600-seconds
const delayTopicNameFormat = "%v-delay-%v-seconds"

const (
	defaultMinimumBackoffInSeconds    uint = 10
	lowerBoundMinimumBackoffInSeconds uint = 0
	upperBoundMinimumBackoffInSeconds uint = 600

	defaultMaximumBackoffInSeconds    uint = 600
	lowerBoundMaximumBackoffInSeconds uint = 0
	upperBoundMaximumBackoffInSeconds uint = 600

	defaultMaxDeliveryAttempts    int32 = 5
	lowerBoundMaxDeliveryAttempts int32 = 5
	upperBoundMaxDeliveryAttempts int32 = 100
)

var (
	// ErrInvalidMinBackoff ...
	ErrInvalidMinBackoff = errors.New("min backoff should be between 0 and 600 seconds")
	// ErrInvalidMaxBackoff ...
	ErrInvalidMaxBackoff = errors.New("max backoff should be between 0 and 600 seconds")
	// ErrInvalidMaxDeliveryAttempt ...
	ErrInvalidMaxDeliveryAttempt = errors.New("max delivery attempt should be between 5 and 100")
)

// Interval is internal delay type per allowed interval
type Interval uint

var (
	// Delay5sec ...
	Delay5sec Interval = 5
	// Delay10sec ...
	Delay10sec Interval = 10
	// Delay30sec ...
	Delay30sec Interval = 30
	// Delay60sec ...
	Delay60sec Interval = 60
	// Delay120sec ...
	Delay120sec Interval = 120
	// Delay150sec ...
	Delay150sec Interval = 150
	// Delay300sec ...
	Delay300sec Interval = 300
	// Delay450sec ...
	Delay450sec Interval = 450
	// Delay600sec ...
	Delay600sec Interval = 600
)

// Intervals during subscription creation, query from the allowed intervals list, and create all the needed topics for retry.
var Intervals = []Interval{Delay5sec, Delay10sec, Delay30sec, Delay60sec, Delay120sec, Delay150sec, Delay300sec, Delay450sec, Delay600sec}

// DelayConsumerConfig ...
type DelayConsumerConfig struct {
	Topic           string `json:"topic"`
	GroupID         string `json:"group_id"`
	GroupInstanceID string `json:"group_instance_id"`
}

// DelayConfig ...
type DelayConfig struct {
	MinimumBackoffInSeconds uint     `json:"minimum_backoff_in_seconds"`
	MaximumBackoffInSeconds uint     `json:"maximum_backoff_in_seconds"`
	MaxDeliveryAttempts     int32    `json:"max_delivery_attempts"`
	Subscription            string   `json:"subscription"`
	DeadLetterTopic         string   `json:"dead_letter_topic"`
	DelayTopics             []string `json:"delay_topics"`
	// maintains a mapping for all the delay intervals to its delay topic name. This will be used to lookup the next topic name.
	DelayIntervalToTopicMap map[Interval]DelayConsumerConfig `json:"delay_interval_to_topic_map"`
}

// NewDelayConfig validates a subscription model and initializes the needed config values to be used for delay consumers
func NewDelayConfig(model *Model) (*DelayConfig, error) {

	minB := defaultMinimumBackoffInSeconds
	maxB := defaultMaximumBackoffInSeconds
	maxAtt := defaultMaxDeliveryAttempts

	if model.RetryPolicy != nil {
		minB = model.RetryPolicy.MinimumBackoff
		if minB < lowerBoundMinimumBackoffInSeconds || minB > upperBoundMinimumBackoffInSeconds {
			return nil, ErrInvalidMinBackoff
		}
	}

	if model.RetryPolicy != nil {
		maxB = model.RetryPolicy.MaximumBackoff
		if maxB < lowerBoundMaximumBackoffInSeconds || maxB > upperBoundMaximumBackoffInSeconds {
			return nil, ErrInvalidMaxBackoff
		}
	}

	// currently dl-topics are auto-created for subscriptions, refer GetValidatedModelForCreate()
	if model.DeadLetterPolicy != nil {
		maxAtt = model.DeadLetterPolicy.MaxDeliveryAttempts
		if maxAtt == 0 {
			// since dl-topics are auto-created, a user may never provide a dead-letter policy during subscription creation.
			// in such cases we set MaxDeliveryAttempts to a default value
			maxAtt = defaultMaxDeliveryAttempts
		} else if maxAtt < lowerBoundMaxDeliveryAttempts || maxAtt > upperBoundMaxDeliveryAttempts {
			return nil, ErrInvalidMaxDeliveryAttempt
		}
	}

	delayTopics := make([]string, 0)
	delayIntervalToTopicNameMap := make(map[Interval]DelayConsumerConfig)
	for _, interval := range Intervals {
		delayTopic := topic.GetTopicName(model.ExtractedSubscriptionProjectID, fmt.Sprintf(delayTopicNameFormat, model.ExtractedSubscriptionName, interval))
		delayTopics = append(delayTopics, delayTopic)
		delayIntervalToTopicNameMap[interval] = DelayConsumerConfig{
			Topic:           delayTopic,
			GroupID:         model.ExtractedSubscriptionName,
			GroupInstanceID: uuid.New().String(),
		}
	}

	config := &DelayConfig{
		MinimumBackoffInSeconds: minB,
		MaximumBackoffInSeconds: maxB,
		MaxDeliveryAttempts:     maxAtt,
		Subscription:            model.ExtractedSubscriptionName,
		DeadLetterTopic:         model.GetDeadLetterTopic(),
		DelayTopics:             delayTopics,
		DelayIntervalToTopicMap: delayIntervalToTopicNameMap,
	}

	return config, nil
}

// GetMinimumBackoffInSeconds ...
func (rc *DelayConfig) GetMinimumBackoffInSeconds() uint {
	return rc.MinimumBackoffInSeconds
}

// GetMaximumBackoffInSeconds ...
func (rc *DelayConfig) GetMaximumBackoffInSeconds() uint {
	return rc.MaximumBackoffInSeconds
}

// GetMaxDeliveryAttempts ...
func (rc *DelayConfig) GetMaxDeliveryAttempts() int32 {
	return rc.MaxDeliveryAttempts
}

// GetSubscription ...
func (rc *DelayConfig) GetSubscription() string {
	return rc.Subscription
}

// GetDeadLetterTopic ...
func (rc *DelayConfig) GetDeadLetterTopic() string {
	return rc.DeadLetterTopic
}

// GetDelayTopics ...
func (rc *DelayConfig) GetDelayTopics() []string {
	return rc.DelayTopics
}

// GetDelayTopicsMap ...
func (rc *DelayConfig) GetDelayTopicsMap() map[Interval]DelayConsumerConfig {
	return rc.DelayIntervalToTopicMap
}

// GetDelayTopicForInterval ...
func (rc *DelayConfig) GetDelayTopicForInterval(interval Interval) string {
	return rc.DelayIntervalToTopicMap[interval].Topic
}
