package subscription

import (
	"github.com/razorpay/metro/internal/common"
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
	ExtractedTopicProjectID        string
	ExtractedSubscriptionProjectID string
	ExtractedTopicName             string
	ExtractedSubscriptionName      string
	// TODO: add remaining fields from spec.proto
}

// Key returns the key for storing subscriptions in the registry
func (m *Model) Key() string {
	return m.Prefix() + m.ExtractedSubscriptionName
}

// Prefix returns the key prefix
func (m *Model) Prefix() string {
	return common.BasePrefix + Prefix + m.ExtractedSubscriptionProjectID + "/"
}
