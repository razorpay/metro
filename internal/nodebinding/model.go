package nodebinding

import (
	"strings"
	"time"

	"github.com/razorpay/metro/internal/common"
)

const (
	// Prefix for all node keys in the registry
	Prefix = "nodebinding/"
)

// Model for a node
type Model struct {
	common.BaseModel
	ID             string
	NodeID         string
	SubscriptionID string
	ScheduledAt    time.Time
}

// Key returns the key for storing in the registry
func (m *Model) Key() string {
	subID := strings.ReplaceAll(m.SubscriptionID, "/", "_")
	return m.Prefix() + m.NodeID + "/" + subID + "_" + m.ID[0:4]
}

// Prefix returns the key prefix
func (m *Model) Prefix() string {
	return common.GetBasePrefix() + Prefix
}
