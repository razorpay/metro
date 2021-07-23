package subscription

import (
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
	ExtractedTopicProjectID        string            `json:"-"`
	ExtractedSubscriptionProjectID string            `json:"-"`
	ExtractedTopicName             string            `json:"-"`
	ExtractedSubscriptionName      string            `json:"-"`
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
