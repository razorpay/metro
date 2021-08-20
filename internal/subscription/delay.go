package subscription

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/razorpay/metro/internal/topic"
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
	lowerBoundMaxDeliveryAttempts int32 = 1
	upperBoundMaxDeliveryAttempts int32 = 100
)

var (
	// ErrInvalidMinBackoff ...
	ErrInvalidMinBackoff = errors.New(fmt.Sprintf("min backoff should be between %v and %v seconds", lowerBoundMinimumBackoffInSeconds, upperBoundMinimumBackoffInSeconds))
	// ErrInvalidMaxBackoff ...
	ErrInvalidMaxBackoff = errors.New(fmt.Sprintf("max backoff should be between %v and %v seconds", lowerBoundMaximumBackoffInSeconds, upperBoundMaximumBackoffInSeconds))
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

// DelayConsumerConfig ...
type DelayConsumerConfig struct {
	Topic           string `json:"topic"`
	GroupID         string `json:"group_id"`
	GroupInstanceID string `json:"group_instance_id"`
}

// LogFields ...
func (dc DelayConsumerConfig) LogFields(kv ...interface{}) []interface{} {
	fields := []interface{}{
		"delayConsumerConfig", map[string]interface{}{
			"topic":           dc.Topic,
			"groupID":         dc.GroupID,
			"groupInstanceID": dc.GroupInstanceID,
		},
	}

	fields = append(fields, kv...)
	return fields
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
	groupID := uuid.New().String()
	for _, interval := range Intervals {
		delayTopic := topic.GetTopicName(model.ExtractedSubscriptionProjectID, fmt.Sprintf(delayTopicNameFormat, model.ExtractedSubscriptionName, interval))
		delayTopics = append(delayTopics, delayTopic)
		delayIntervalToTopicNameMap[interval] = DelayConsumerConfig{
			Topic:           delayTopic,
			GroupID:         groupID,
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
