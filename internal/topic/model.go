package topic

import (
	"fmt"
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

	// MaxNumPartitions max number of partitions for a topic
	MaxNumPartitions = 100

	// RetentionPeriodConfig is the name of topic level retention period config property
	RetentionPeriodConfig = "retention.ms"

	// RetentionPeriod is the time after which messages will be deleted from the topic = 3 days
	RetentionPeriod = 1000 * 60 * 60 * 24 * 14

	// RetentionSizeConfig is the name of topic level retention by size config property
	RetentionSizeConfig = "retention.bytes"

	// RetentionSizePerPartition is the max no of bytes retained per topic = 10000MB
	RetentionSizePerPartition = 10000 * 1000000
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

// IsDelayTopic checks if the topic is a delay topic created for delay support in subscription
func (m *Model) IsDelayTopic() bool {
	for _, interval := range Intervals {
		delaySuffix := fmt.Sprintf(DelayTopicSuffix, interval)
		if strings.HasSuffix(m.ExtractedTopicName, delaySuffix) {
			return true
		}
	}
	return false
}

// IsRetryTopic checks if the topic is a retry topic created for retry support in subscription
func (m *Model) IsRetryTopic() bool {
	return strings.HasSuffix(m.ExtractedTopicName, RetryTopicSuffix)
}

// IsPrimaryTopic checks if the topic is primary topic or not
func (m *Model) IsPrimaryTopic() bool {
	return !m.IsDeadLetterTopic() && !m.IsDelayTopic() && !m.IsRetryTopic()
}

// GetRetentionConfig returns the retention policy for a given topic
func (m *Model) GetRetentionConfig() map[string]string {
	if m.IsDeadLetterTopic() {
		return map[string]string{
			RetentionPeriodConfig: fmt.Sprint(RetentionPeriod),
			RetentionSizeConfig:   fmt.Sprint(RetentionSizePerPartition),
		}
	}
	return nil
}
