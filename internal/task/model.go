package task

import (
	"github.com/razorpay/metro/internal/common"
)

const (
	// Prefix for all tasks keys in the registry
	Prefix = "tasks/"
)

// Model for a task
type Model struct {
	common.BaseModel
	ID               string
	TaskGroupID      string
	Broker           string
	Topic            string
	URL              string
	NodeID           string
	Status           string
	ErrorDescription string
}

// Key returns the key for storing tasks in the registry
func (m *Model) Key() string {
	return m.Prefix() + m.ID
}

// Prefix returns the key prefix
func (m *Model) Prefix() string {
	return common.BasePrefix + "/" + Prefix
}
