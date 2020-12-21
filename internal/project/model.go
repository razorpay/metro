package project

import (
	"github.com/razorpay/metro/internal/common"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

const (
	projectPrefix = "projects/"
)

// Model for a project
type Model struct {
	common.BaseModel
	Name      string            `json:"name"`
	ProjectID string            `json:"projectId"`
	Labels    map[string]string `json:"labels"`
}

// FromProto creates and returns a Model from proto message
func FromProto(proto *metrov1.Project) *Model {
	m := &Model{}
	m.Name = proto.GetName()
	m.ProjectID = proto.GetProjectId()
	m.Labels = proto.GetLabels()
	return m
}

// Key returns the key for storing in the registry
func (m *Model) Key() string {
	return m.Prefix() + m.ProjectID
}

// Prefix returns the key prefix
func (m *Model) Prefix() string {
	return m.BaseModel.Prefix() + projectPrefix
}
