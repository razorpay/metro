package project

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/razorpay/metro/internal/auth"
	"github.com/razorpay/metro/internal/common"
	"github.com/sethvargo/go-password/password"
)

const (
	// Prefix for all project keys in the registry
	Prefix = "projects/"

	// PartSeparator used in constructing the username parts
	PartSeparator = "__"

	// perf10__656f81 : sample username format
	usernameFormat = "%v%v%v"
)

// Model for a project
type Model struct {
	common.BaseModel
	Name      string                `json:"name"`
	ProjectID string                `json:"projectId"`
	Labels    map[string]string     `json:"labels"`
	AppAuth   map[string]*auth.Auth `json:"app_auth"`
}

// Key returns the key for storing in the registry
func (m *Model) Key() string {
	return m.Prefix() + m.ProjectID
}

// Prefix returns the key prefix
func (m *Model) Prefix() string {
	return common.GetBasePrefix() + Prefix
}

// NewAuthKey generate new auth keys to access project
func (m *Model) NewAuthKey() *auth.Auth {
	username := fmt.Sprintf(usernameFormat, m.ProjectID, PartSeparator, uuid.New().String()[:6])
	pwd, _ := password.Generate(20, 10, 0, false, true)
	authM := auth.NewAuth(username, pwd)
	m.AppAuth[username] = authM
	return authM
}

// DeleteAuthKey deletes an existing auth key from the project. Returns true if it was successfully able to complete the operation.
func (m *Model) DeleteAuthKey(username string) bool {
	if m.AppAuth == nil || len(m.AppAuth) == 0 {
		return false
	}

	if _, ok := m.AppAuth[username]; ok {
		delete(m.AppAuth, username)
		return true
	}

	return false
}
