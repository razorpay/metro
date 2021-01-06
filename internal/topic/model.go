package topic

import (
	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/internal/project"
)

const (
	// Prefix for all topic keys in the registry
	Prefix = "topics/"
)

// Model for a topic
type Model struct {
	common.BaseModel
	Name               string
	Labels             map[string]string
	ExtractedProjectID string
	ExtractedTopicName string
}

// Key returns the key for storing in the registry
func (m *Model) Key() string {
	return m.Prefix() + m.ExtractedTopicName
}

// Prefix returns the key prefix
func (m *Model) Prefix() string {
	return common.BasePrefix + project.Prefix + m.ExtractedProjectID + "/" + Prefix
}
