package common

import (
	"github.com/razorpay/metro/internal/app"
)

const (
	defaultVersion string = "0"
)

// IModel interface which all models should implement
type IModel interface {
	// Key returns the key to store the model against
	Key() string
	// Prefix returns the key prefix used by the module
	Prefix() string
	// Returns the version of the saved model in the registry. Version changes for every update.
	GetVersion() string
	//Sets the version of the saved model from the registry. Never to be used by business logic.
	SetVersion(version string)
}

// BaseModel implements basic model functionality
type BaseModel struct {
	version *string
}

// SetVersion Base implementation of set version
func (b *BaseModel) SetVersion(version string) {
	b.version = &version
}

// GetVersion Base implementation of get version. Default version is "0".
func (b *BaseModel) GetVersion() string {
	if b.version == nil {
		return defaultVersion
	}
	return *b.version
}

// GetBasePrefix returns the prefix
func GetBasePrefix() string {
	return "metro" + "-" + app.GetEnv() + "/"
}
