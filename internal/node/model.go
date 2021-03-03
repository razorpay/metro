package node

import (
	"github.com/razorpay/metro/internal/common"
)

const (
	// Prefix for all nods keys in the registry
	Prefix = "nodes/"
)

// Model for a node
type Model struct {
	common.BaseModel
	ID string
}

// Key returns the key for storing in the registry
func (m *Model) Key() string {
	return m.Prefix() + m.ID
}

// Prefix returns the key prefix
func (m *Model) Prefix() string {
	return common.BasePrefix + Prefix
}
