package topic

import (
	"strings"

	"github.com/razorpay/metro/internal/common"
)

const (
	// Prefix for all topic keys in the registry
	Prefix = "topics/"

	// RetryTopicSuffix every primary topic subscription will have a retry topic with this suffix as well
	RetryTopicSuffix = "-retry"

	// DeadLetterTopicSuffix every primary topic subscription will have a dlq topic with this suffix as well
	DeadLetterTopicSuffix = "-dlq"

	// SubscriptionSuffix is the suffix to be appended to the subscription specific topic
	SubscriptionSuffix = "-subscription-internal"

	// DefaultNumPartitions default no of partitions for a topic
	DefaultNumPartitions = 1
)

// Model for a topic
type Model struct {
	common.BaseModel
	Name               string            `json:"name"`
	Labels             map[string]string `json:"labels"`
	ExtractedProjectID string            `json:"extracted_project_id"`
	ExtractedTopicName string            `json:"extracted_topic_name"`
	NumPartitions      int               `json:"num_partitions"`
}

// Key returns the key for storing in the registry
func (m *Model) Key() string {
	return m.Prefix() + m.ExtractedTopicName
}

// Prefix returns the key prefix
func (m *Model) Prefix() string {
	return common.GetBasePrefix() + Prefix + m.ExtractedProjectID + "/"
}

// IsDeadLetterTopic checks if the topic is a dead letter topic created for dlq support on subscription
func (m *Model) IsDeadLetterTopic() bool {
	return strings.HasSuffix(m.ExtractedTopicName, DeadLetterTopicSuffix)
}
