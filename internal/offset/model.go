package offset

import (
	"strconv"

	"github.com/razorpay/metro/internal/common"
)

const (
	// Prefix for all offset keys in the registry
	offsetPrefix       = "offsets/"
	subscriptionPrefix = "/subscripitons/"
	partitionPrefix    = "/partitions/"
)

// Model for an offset
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
	return m.Prefix() + m.Topic + subscriptionPrefix + m.Subscription + partitionPrefix + strconv.Itoa(int(m.Partition))
}

// Prefix returns the key prefix
func (m *Model) Prefix() string {
	return common.GetBasePrefix() + offsetPrefix
}
