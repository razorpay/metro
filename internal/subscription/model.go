package subscription

import (
	"fmt"
	urlpkg "net/url"

	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/internal/credentials"
	"github.com/razorpay/metro/internal/topic"
	filter "github.com/razorpay/metro/pkg/filtering"
)

const (
	// Prefix for all subscriptions keys in the registry
	Prefix = "subscriptions/"

	// SubscriptionTypePush ...
	SubscriptionTypePush = "Push"

	// SubscriptionTypePull ...
	SubscriptionTypePull = "Pull"
)

// Model for a subscription
type Model struct {
	common.BaseModel
	Name                     string            `json:"name,omitempty"`
	Topic                    string            `json:"topic,omitempty"`
	PushConfig               *PushConfig       `json:"push_config,omitempty"`
	AckDeadlineSeconds       int32             `json:"ack_deadline_seconds,omitempty"`
	RetainAckedMessages      bool              `json:"retain_acked_messages,omitempty"`
	MessageRetentionDuration uint              `json:"message_retention_duration,omitempty"`
	Labels                   map[string]string `json:"labels,omitempty"`
	EnableMessageOrdering    bool              `json:"enable_message_ordering,omitempty"`
	ExpirationPolicy         *ExpirationPolicy `json:"expiration_policy,omitempty"`
	// use SetFilterExpression function for setting FilterExpression field
	FilterExpression               string `json:"filter,omitempty"`
	filterStruct                   *Filter
	RetryPolicy                    *RetryPolicy      `json:"retry_policy,omitempty"`
	DeadLetterPolicy               *DeadLetterPolicy `json:"dead_letter_policy,omitempty"`
	Detached                       bool              `json:"detached,omitempty"`
	ExtractedTopicProjectID        string            `json:"extracted_topic_project_id"`
	ExtractedSubscriptionProjectID string            `json:"extracted_subscription_project_id"`
	ExtractedTopicName             string            `json:"extracted_topic_name"`
	ExtractedSubscriptionName      string            `json:"extracted_subscription_name"`
}

// Filter defines the Filter criteria for messages
type Filter = filter.Condition

// PushConfig defines the push endpoint
type PushConfig struct {
	PushEndpoint string             `json:"push_endpoint,omitempty"`
	Attributes   map[string]string  `json:"attributes,omitempty"`
	Credentials  *credentials.Model `json:"credentials,omitempty"`
}

// GetRedactedPushEndpoint returns the push endpoint but replaces any password with "xxxxx".
func (m *Model) GetRedactedPushEndpoint() string {
	if m.IsPush() {
		if url, err := urlpkg.ParseRequestURI(m.PushConfig.PushEndpoint); err == nil {
			return url.Redacted()
		}
	}
	return ""
}

// RetryPolicy defines the retry policy
type RetryPolicy struct {
	MinimumBackoff uint `json:"minimum_backoff,omitempty"`
	MaximumBackoff uint `json:"maximum_backoff,omitempty"`
}

// ExpirationPolicy defines the expiration policy
type ExpirationPolicy struct {
	TTL uint `json:"ttl,omitempty"`
}

// DeadLetterPolicy defines the dead letter policy
type DeadLetterPolicy struct {
	// DeadLetterTopic keeps the topic name used for dead lettering, this will be created with subscription and
	// will be visible to subscriber, subscriber can create subscription over this topic to read messages from this
	DeadLetterTopic     string `json:"dead_letter_topic,omitempty"`
	MaxDeliveryAttempts int32  `json:"max_delivery_attempts,omitempty"`
}

// Key returns the Key for storing subscriptions in the registry
func (m *Model) Key() string {
	return m.Prefix() + m.ExtractedSubscriptionName
}

// Prefix returns the Key prefix
func (m *Model) Prefix() string {
	return common.GetBasePrefix() + Prefix + m.ExtractedSubscriptionProjectID + "/"
}

// IsPush returns true if a subscription is a push subscription
func (m *Model) IsPush() bool {
	return m.PushConfig != nil && m.PushConfig.PushEndpoint != ""
}

// GetSubscriptionType returns subscription type i.e. Push or Pull
func (m *Model) GetSubscriptionType() string {
	if m.IsPush() {
		return SubscriptionTypePush
	}
	return SubscriptionTypePull
}

// GetTopic returns the primary subscription topic
func (m *Model) GetTopic() string {
	return m.Topic
}

// GetSubscriptionTopic returns the topic used for subscription fanout topic
func (m *Model) GetSubscriptionTopic() string {
	return topic.GetTopicName(m.ExtractedSubscriptionProjectID, m.ExtractedSubscriptionName+topic.SubscriptionSuffix)
}

// GetRetryTopic returns the topic used for subscription retries
func (m *Model) GetRetryTopic() string {
	return topic.GetTopicName(m.ExtractedSubscriptionProjectID, m.ExtractedSubscriptionName+topic.RetryTopicSuffix)
}

// GetDeadLetterTopic returns the topic used for dead lettering for subscription
func (m *Model) GetDeadLetterTopic() string {
	if m.DeadLetterPolicy == nil || m.DeadLetterPolicy.DeadLetterTopic == "" {
		// adding this for backward compatibility as older models will not have persisted DLQ topic name
		return topic.GetTopicName(m.ExtractedTopicProjectID, m.ExtractedSubscriptionName+topic.DeadLetterTopicSuffix)
	}
	return m.DeadLetterPolicy.DeadLetterTopic
}

// GetCredentials returns the credentials for the push endpoint
func (m *Model) GetCredentials() *credentials.Model {
	if m.PushConfig != nil {
		return m.PushConfig.Credentials
	}
	return nil
}

// HasCredentials returns true if a subscription has credentials for push endpoint
func (m *Model) HasCredentials() bool {
	return m.PushConfig != nil && m.PushConfig.Credentials != nil
}

func (m *Model) setDefaultRetryPolicy() {
	m.RetryPolicy = &RetryPolicy{
		MinimumBackoff: 5,
		MaximumBackoff: 5,
	}
}

func (m *Model) setDefaultDeadLetterPolicy() {
	m.DeadLetterPolicy = &DeadLetterPolicy{
		DeadLetterTopic:     m.GetDeadLetterTopic(),
		MaxDeliveryAttempts: 5,
	}
}

// GetDelayTopicsMap returns the all the delay topic names to its interval map
func (m *Model) GetDelayTopicsMap() map[topic.Interval]string {
	return map[topic.Interval]string{
		topic.Delay5sec:    m.GetDelay5secTopic(),
		topic.Delay30sec:   m.GetDelay30secTopic(),
		topic.Delay60sec:   m.GetDelay60secTopic(),
		topic.Delay150sec:  m.GetDelay150secTopic(),
		topic.Delay300sec:  m.GetDelay300secTopic(),
		topic.Delay600sec:  m.GetDelay600secTopic(),
		topic.Delay1800sec: m.GetDelay1800secTopic(),
		topic.Delay3600sec: m.GetDelay3600secTopic(),
	}
}

// GetDelayTopics returns the all the delay topic names for this subscription
func (m *Model) GetDelayTopics() []string {
	return []string{
		m.GetDelay5secTopic(),
		m.GetDelay30secTopic(),
		m.GetDelay60secTopic(),
		m.GetDelay150secTopic(),
		m.GetDelay300secTopic(),
		m.GetDelay600secTopic(),
		m.GetDelay1800secTopic(),
		m.GetDelay3600secTopic(),
	}
}

// GetDelayTopicsByBackoff returns delay topic based on retry policy
func (m *Model) GetDelayTopicsByBackoff() []string {
	if m.DeadLetterPolicy == nil || m.RetryPolicy == nil {
		return m.GetDelayTopics()
	}

	delayTopicsMap := m.GetDelayTopicsMap()
	nef := m.GetBackoff()
	finder := m.GetIntervalFinder()
	currentRetryCount := 1
	currentInterval := 0
	delayTopics := make([]string, 0)

	for currentRetryCount <= int(m.DeadLetterPolicy.MaxDeliveryAttempts) {
		nextDelayInterval := nef.Next(NewBackoffPolicy(
			float64(m.RetryPolicy.MinimumBackoff),
			float64(currentInterval),
			float64(currentRetryCount),
			DefaultBackoffExponential,
		))

		closestInterval := finder.Next(NewIntervalFinderParams(
			m.RetryPolicy.MinimumBackoff,
			m.RetryPolicy.MaximumBackoff,
			nextDelayInterval,
			topic.Intervals,
		))

		delayTopics = append(delayTopics, delayTopicsMap[closestInterval])
		currentInterval = int(nextDelayInterval)
		currentRetryCount++
	}
	return delayTopics
}

// GetBackoff returns backoff policy
func (m *Model) GetBackoff() Backoff {
	return NewExponentialWindowBackoff()
}

// GetIntervalFinder returns the interval window finder
func (m *Model) GetIntervalFinder() IntervalFinder {
	return NewClosestIntervalWithCeil()
}

// GetDelay5secTopic returns the formatted delay topic name for 5sec interval
func (m *Model) GetDelay5secTopic() string {
	return fmt.Sprintf(topic.DelayTopicWithProjectNameFormat, m.ExtractedSubscriptionProjectID, m.ExtractedSubscriptionName, topic.Delay5sec)
}

// GetDelay30secTopic returns the formatted delay topic name for 30sec interval
func (m *Model) GetDelay30secTopic() string {
	return fmt.Sprintf(topic.DelayTopicWithProjectNameFormat, m.ExtractedSubscriptionProjectID, m.ExtractedSubscriptionName, topic.Delay30sec)
}

// GetDelay60secTopic returns the formatted delay topic name for 60sec interval
func (m *Model) GetDelay60secTopic() string {
	return fmt.Sprintf(topic.DelayTopicWithProjectNameFormat, m.ExtractedSubscriptionProjectID, m.ExtractedSubscriptionName, topic.Delay60sec)
}

// GetDelay150secTopic returns the formatted delay topic name for 150sec interval
func (m *Model) GetDelay150secTopic() string {
	return fmt.Sprintf(topic.DelayTopicWithProjectNameFormat, m.ExtractedSubscriptionProjectID, m.ExtractedSubscriptionName, topic.Delay150sec)
}

// GetDelay300secTopic returns the formatted delay topic name for 300sec interval
func (m *Model) GetDelay300secTopic() string {
	return fmt.Sprintf(topic.DelayTopicWithProjectNameFormat, m.ExtractedSubscriptionProjectID, m.ExtractedSubscriptionName, topic.Delay300sec)
}

// GetDelay600secTopic returns the formatted delay topic name for 600sec interval
func (m *Model) GetDelay600secTopic() string {
	return fmt.Sprintf(topic.DelayTopicWithProjectNameFormat, m.ExtractedSubscriptionProjectID, m.ExtractedSubscriptionName, topic.Delay600sec)
}

// GetDelay1800secTopic returns the formatted delay topic name for 1800sec interval
func (m *Model) GetDelay1800secTopic() string {
	return fmt.Sprintf(topic.DelayTopicWithProjectNameFormat, m.ExtractedSubscriptionProjectID, m.ExtractedSubscriptionName, topic.Delay1800sec)
}

// GetDelay3600secTopic returns the formatted delay topic name for 3600sec interval
func (m *Model) GetDelay3600secTopic() string {
	return fmt.Sprintf(topic.DelayTopicWithProjectNameFormat, m.ExtractedSubscriptionProjectID, m.ExtractedSubscriptionName, topic.Delay3600sec)
}

// GetDelayConsumerGroupID returns the consumer group ID to be used by the delay consumers
func (m *Model) GetDelayConsumerGroupID(delayTopic string) string {
	return fmt.Sprintf(topic.DelayConsumerGroupIDFormat, delayTopic)
}

// GetDelayConsumerGroupInstanceID returns the consumer group ID to be used by the specific delay consumer
func (m *Model) GetDelayConsumerGroupInstanceID(subscriberID, delayTopic string) string {
	return fmt.Sprintf(topic.DelayConsumerGroupInstanceIDFormat, delayTopic, subscriberID)
}

// GetFilterExpressionAsStruct parses and returns the filter expression into GO Struct
func (m *Model) GetFilterExpressionAsStruct() (*Filter, error) {
	if m.filterStruct != nil {
		return m.filterStruct, nil
	}
	f := &Filter{}
	err := filter.Parser.ParseString("", m.FilterExpression, f)
	if err != nil {
		return nil, err
	}
	m.filterStruct = f
	return f, nil
}

// SetFilterExpression sets filter expression to the new input.
// It also sets filter struct to nil. It will be set greedily whenever required
func (m *Model) SetFilterExpression(Filter string) {
	m.FilterExpression = Filter
	m.filterStruct = nil
}

// IsFilteringEnabled checks if the subscription has filter criteria or not
func (m *Model) IsFilteringEnabled() bool {
	return len(m.FilterExpression) > 0
}
