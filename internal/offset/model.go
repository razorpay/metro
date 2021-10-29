package offset

import (
	"strconv"

	"github.com/razorpay/metro/internal/common"
)

const (
	// Prefix for all project keys in the registry
	OffsetPrefix       = "/offsets"
	SubscriptionPrefix = "/subscripitons/"
	PartitionPrefix    = "/partitions/"
)

// Model for a project
type Model struct {
	common.BaseModel
	Topic          string `json:"topic_id"`
	Subscription   string `json:"subscription_id"`
	Partition      int32  `json:"partition_id"`
	LatestOffset   string
	rollbackOffset string
}

// Key returns the key for storing in the registry
func (m *Model) Key() string {
	return m.Prefix() + m.Topic + SubscriptionPrefix + m.Subscription + PartitionPrefix + strconv.Itoa(int(m.Partition))
}

// Prefix returns the key prefix
func (m *Model) Prefix() string {
	return common.GetBasePrefix() + OffsetPrefix
}
