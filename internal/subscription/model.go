package subscription

import (
	"strings"

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
	Name                           string
	Topic                          string
	Labels                         map[string]string
	PushEndpoint                   string
	AckDeadlineSec                 int32
	ExtractedTopicProjectID        string
	ExtractedSubscriptionProjectID string
	ExtractedTopicName             string
	ExtractedSubscriptionName      string
	Credentials                    credentials.ICredentials

	// DeadLetterTopic keeps the topic name used for dead lettering, this will be created with subscription and
	// will be visible to subscriber, subscriber can create subscription over this topic to read messages from this
	DeadLetterTopic string

	// TODO: add remaining fields from spec.proto
}

// NormalizedKey returns the Key with '/' replaced with '_' to be used safely within the broker constructs
func (m *Model) NormalizedKey() string {
	return strings.Replace(m.Key(), "/", "_", -1)
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
func (m *Model) GetCredentials() credentials.ICredentials {
	return m.Credentials
}

// HasCredentials returns true if a subscription has credentials for push endpoint
func (m *Model) HasCredentials() bool {
	return m.Credentials != nil
}
