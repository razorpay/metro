package nodebinding

import (
	"github.com/razorpay/metro/internal/common"
)

const (
	// Prefix for all nods keys in the registry
	Prefix = "nodebinding/"
)

// Model for a node
type Model struct {
	common.BaseModel
	NodeID         string
	SubscriptionID string
}

// Key returns the key for storing in the registry
func (m *Model) Key() string {
	return m.Prefix() + m.NodeID + "/" + m.SubscriptionID
}

// Prefix returns the key prefix
func (m *Model) Prefix() string {
	return common.BasePrefix + Prefix
}
