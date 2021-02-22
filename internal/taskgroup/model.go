package taskgroup

import (
	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/internal/project"
)

const (
	// Prefix for all taskgroup keys in the registry
	Prefix = "taskgroups/"
)

// Model for a taskgroup
type Model struct {
	common.BaseModel
	ID           string
	Topic        string
	Broker       string
	CurrentCount int64
	MaxCount     int64
	MinCount     int64
}

// Key returns the key for storing taskgroups in the registry
func (m *Model) Key() string {
	return m.Prefix() + m.ID
}

// Prefix returns the key prefix
func (m *Model) Prefix() string {
	return common.BasePrefix + project.Prefix + m.ID + "/"
}
