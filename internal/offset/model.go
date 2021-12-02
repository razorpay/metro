package offset

import (
	"strconv"

	"github.com/razorpay/metro/internal/common"
)

const (
	// Prefix for all offset keys in the registry
	offsetPrefix       = "offsets/"
	offsetStatusPrefix = "offset-status/"
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
	LatestOffset   int32  `json:"latest_offset_value"`
	OrderingKey    string `json:"ordering_key"`
	rollbackOffset int32
}

// Status denotes the delivery status for an offset
type Status struct {
	Model
	OffsetStatus string `json:"status,omitempty"`
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

// Key returns the key for storing in the registry
func (m *Status) Key() string {
	key := m.Prefix() + m.Topic + subscriptionPrefix + m.Subscription + partitionPrefix + strconv.Itoa(int(m.Partition))
	if len(m.OrderingKey) > 0 {
		key = key + orderingPrefix + m.OrderingKey
	}
	return key
}

// Prefix returns the key prefix
func (m *Status) Prefix() string {
	return common.GetBasePrefix() + offsetStatusPrefix
}
