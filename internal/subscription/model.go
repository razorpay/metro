package subscription

import (
	"fmt"

	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/internal/credentials"
	"github.com/razorpay/metro/internal/topic"
)

const (
	// Prefix for all subscriptions keys in the registry
	Prefix = "subscriptions/"
)

// Model for a subscription
type Model struct {
	common.BaseModel
	Name                           string            `json:"name,omitempty"`
	Topic                          string            `json:"topic,omitempty"`
	PushConfig                     *PushConfig       `json:"push_config,omitempty"`
	AckDeadlineSeconds             int32             `json:"ack_deadline_seconds,omitempty"`
	RetainAckedMessages            bool              `json:"retain_acked_messages,omitempty"`
	MessageRetentionDuration       uint              `json:"message_retention_duration,omitempty"`
	Labels                         map[string]string `json:"labels,omitempty"`
	EnableMessageOrdering          bool              `json:"enable_message_ordering,omitempty"`
	ExpirationPolicy               *ExpirationPolicy `json:"expiration_policy,omitempty"`
	Filter                         string            `json:"filter,omitempty"`
	RetryPolicy                    *RetryPolicy      `json:"retry_policy,omitempty"`
	DeadLetterPolicy               *DeadLetterPolicy `json:"dead_letter_policy,omitempty"`
	Detached                       bool              `json:"detached,omitempty"`
	ExtractedTopicProjectID        string            `json:"extracted_topic_project_id"`
	ExtractedSubscriptionProjectID string            `json:"extracted_subscription_project_id"`
	ExtractedTopicName             string            `json:"extracted_topic_name"`
	ExtractedSubscriptionName      string            `json:"extracted_subscription_name"`
}

// PushConfig defines the push endpoint
type PushConfig struct {
	PushEndpoint string             `json:"push_endpoint,omitempty"`
	Attributes   map[string]string  `json:"attributes,omitempty"`
	Credentials  *credentials.Model `json:"credentials,omitempty"`
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

// GetTopic returns the primary subscription topic
func (m *Model) GetTopic() string {
	return m.Topic
}

// GetRetryTopic returns the topic used for subscription retries
func (m *Model) GetRetryTopic() string {
	return topic.GetTopicName(m.ExtractedTopicProjectID, m.ExtractedSubscriptionName+topic.RetryTopicSuffix)
}

// GetDeadLetterTopic returns the topic used for dead lettering for subscription
func (m *Model) GetDeadLetterTopic() string {
	if m.DeadLetterPolicy.DeadLetterTopic == "" {
		// adding this for backward compatibility as older models will not have persisted DLQ topic name
		return topic.GetTopicName(m.ExtractedTopicProjectID, m.ExtractedSubscriptionName+topic.DeadLetterTopicSuffix)
	}
	return m.DeadLetterPolicy.DeadLetterTopic
}

// GetCredentials returns the credentials for the push endpoint
func (m *Model) GetCredentials() *credentials.Model {
	return m.PushConfig.Credentials
}

// HasCredentials returns true if a subscription has credentials for push endpoint
func (m *Model) HasCredentials() bool {
	return m.PushConfig.Credentials != nil
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
func (m *Model) GetDelayTopicsMap() map[Interval]string {
	return map[Interval]string{
		Delay5sec:    m.GetDelay5secTopic(),
		Delay30sec:   m.GetDelay30secTopic(),
		Delay60sec:   m.GetDelay60secTopic(),
		Delay150sec:  m.GetDelay150secTopic(),
		Delay300sec:  m.GetDelay300secTopic(),
		Delay600sec:  m.GetDelay600secTopic(),
		Delay1800sec: m.GetDelay1800secTopic(),
		Delay3600sec: m.GetDelay3600secTopic(),
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

// GetDelay5secTopic returns the formatted delay topic name for 5sec interval
func (m *Model) GetDelay5secTopic() string {
	return fmt.Sprintf(delayTopicNameFormat, m.ExtractedSubscriptionProjectID, m.ExtractedSubscriptionName, Delay5sec)
}

// GetDelay30secTopic returns the formatted delay topic name for 30sec interval
func (m *Model) GetDelay30secTopic() string {
	return fmt.Sprintf(delayTopicNameFormat, m.ExtractedSubscriptionProjectID, m.ExtractedSubscriptionName, Delay30sec)
}

// GetDelay60secTopic returns the formatted delay topic name for 60sec interval
func (m *Model) GetDelay60secTopic() string {
	return fmt.Sprintf(delayTopicNameFormat, m.ExtractedSubscriptionProjectID, m.ExtractedSubscriptionName, Delay60sec)
}

// GetDelay150secTopic returns the formatted delay topic name for 150sec interval
func (m *Model) GetDelay150secTopic() string {
	return fmt.Sprintf(delayTopicNameFormat, m.ExtractedSubscriptionProjectID, m.ExtractedSubscriptionName, Delay150sec)
}

// GetDelay300secTopic returns the formatted delay topic name for 300sec interval
func (m *Model) GetDelay300secTopic() string {
	return fmt.Sprintf(delayTopicNameFormat, m.ExtractedSubscriptionProjectID, m.ExtractedSubscriptionName, Delay300sec)
}

// GetDelay600secTopic returns the formatted delay topic name for 600sec interval
func (m *Model) GetDelay600secTopic() string {
	return fmt.Sprintf(delayTopicNameFormat, m.ExtractedSubscriptionProjectID, m.ExtractedSubscriptionName, Delay600sec)
}

// GetDelay1800secTopic returns the formatted delay topic name for 1800sec interval
func (m *Model) GetDelay1800secTopic() string {
	return fmt.Sprintf(delayTopicNameFormat, m.ExtractedSubscriptionProjectID, m.ExtractedSubscriptionName, Delay1800sec)
}

// GetDelay3600secTopic returns the formatted delay topic name for 3600sec interval
func (m *Model) GetDelay3600secTopic() string {
	return fmt.Sprintf(delayTopicNameFormat, m.ExtractedSubscriptionProjectID, m.ExtractedSubscriptionName, Delay3600sec)
}

// GetDelayConsumerGroupID returns the consumer group ID to be used by the delay consumers
func (m *Model) GetDelayConsumerGroupID() string {
	return fmt.Sprintf(delayConsumerGroupIDFormat, m.Name)
}

// GetDelayConsumerGroupInstanceID returns the consumer group ID to be used by the specific delay consumer
func (m *Model) GetDelayConsumerGroupInstanceID(delayTopic string) string {
	return fmt.Sprintf(delayConsumerGroupInstanceIDFormat, delayTopic)
}
