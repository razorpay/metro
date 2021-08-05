package nodebinding

import (
	"strings"

	"github.com/razorpay/metro/internal/common"
)

const (
	// Prefix for all node keys in the registry
	Prefix = "nodebinding/"
)

// Model for a node
type Model struct {
	common.BaseModel
	ID                  string `json:"id"`
	NodeID              string `json:"node_id"`
	SubscriptionID      string `json:"subscription_id"`
	SubscriptionVersion string `json:"subscription_version"`
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
