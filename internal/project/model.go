package project

import (
	"log"
	"strings"

	"github.com/razorpay/metro/internal/common"
)

const (
	// Prefix for all project keys in the registry
	Prefix = "projects/"
)

// Model for a project
type Model struct {
	common.BaseModel
	Name      string            `json:"name"`
	ProjectID string            `json:"project_id"`
	Labels    map[string]string `json:"labels"`
}

// Key returns the key for storing in the registry
func (m *Model) Key() string {
	return m.Prefix() + m.ProjectID
}

// Prefix returns the key prefix
func (m *Model) Prefix() string {
	return common.GetBasePrefix() + Prefix
}

// FetchProjectID returns the key for storing in the registry
func FetchProjectID(val string) string {
	stringArr := strings.Split(val, "/")
	arrLen := len(stringArr)
	if arrLen < 1 {
		log.Fatalf("Invalid ProjectID given as input: [%v]", val)
		return ""
	}
	return stringArr[arrLen-1]
}
