package offset

import (
	"strconv"

	"github.com/razorpay/metro/internal/common"
)

const (
	// Prefix for all offset keys in the registry
	offsetPrefix       = "offsets/"
	subscriptionPrefix = "/subscriptions/"
	partitionPrefix    = "-"
	orderingPrefix     = "/ordering/"
)

// Model for an offset
type Model struct {
	common.BaseModel
	Topic          string `json:"topic_id"`
	Subscription   string `json:"subscription_id"`
	Partition      int32  `json:"partition_id"`
	LatestOffset   int64  `json:"latest_offset"`
	OrderingKey    string `json:"ordering_key"`
	Status         string
	rollbackOffset int64
}

// Key returns the key for storing in the registry
func (m *Model) Key() string {
	key := m.Prefix() + m.Topic + subscriptionPrefix + m.Subscription + partitionPrefix + strconv.Itoa(int(m.Partition))
	if len(m.OrderingKey) > 0 {
		key = key + orderingPrefix + m.OrderingKey
	}
	return key
}

// Prefix returns the key prefix
func (m *Model) Prefix() string {
	return common.GetBasePrefix() + offsetPrefix
}
