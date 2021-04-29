package topic

import (
	"github.com/razorpay/metro/internal/common"
)

const (
	// Prefix for all topic keys in the registry
	Prefix = "topics/"

	// RetryTopicSuffix every primary topic subscription will have a retry topic with this suffix as well
	RetryTopicSuffix = "-retry"

	// DeadLetterTopicSuffix every primary topic subscription will have a dlq topic with this suffix as well
	DeadLetterTopicSuffix = "-dlq"

	// DefaultNumPartitions default no of partitions for a topic
	DefaultNumPartitions = 1
)

// Model for a topic
type Model struct {
	common.BaseModel
	Name               string
	Labels             map[string]string
	ExtractedProjectID string
	ExtractedTopicName string
	NumPartitions      int
}

// Key returns the key for storing in the registry
func (m *Model) Key() string {
	return m.Prefix() + m.ExtractedTopicName
}

// Prefix returns the key prefix
func (m *Model) Prefix() string {
	return common.GetBasePrefix() + Prefix + m.ExtractedProjectID + "/"
}
