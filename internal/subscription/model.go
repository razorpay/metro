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
	Name                           string             `json:"name"`
	Topic                          string             `json:"topic"`
	Labels                         map[string]string  `json:"labels"`
	PushEndpoint                   string             `json:"push_endpoint"`
	AckDeadlineSec                 int32              `json:"ack_deadline_sec"`
	ExtractedTopicProjectID        string             `json:"extracted_topic_project_id"`
	ExtractedSubscriptionProjectID string             `json:"extracted_subscription_project_id"`
	ExtractedTopicName             string             `json:"extracted_topic_name"`
	ExtractedSubscriptionName      string             `json:"extracted_subscription_name"`
	Credentials                    *credentials.Model `json:"credentials"`

	// DeadLetterTopic keeps the topic name used for dead lettering, this will be created with subscription and
	// will be visible to subscriber, subscriber can create subscription over this topic to read messages from this
	DeadLetterTopic string `json:"dead_letter_topic"`

	// TODO: add remaining fields from spec.proto
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
	return m.PushEndpoint != ""
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
	if m.DeadLetterTopic == "" {
		// adding this for backward compatibility as older models will not have persisted DLQ topic name
		return topic.GetTopicName(m.ExtractedTopicProjectID, m.ExtractedSubscriptionName+topic.DeadLetterTopicSuffix)
	}
	return m.DeadLetterTopic
}

// GetCredentials returns the credentials for the push endpoint
func (m *Model) GetCredentials() *credentials.Model {
	return m.Credentials
}

// HasCredentials returns true if a subscription has credentials for push endpoint
func (m *Model) HasCredentials() bool {
	return m.Credentials != nil
}
