package common

import (
	"fmt"

	"github.com/razorpay/metro/internal/app"
)

// IModel interface which all models should implement
type IModel interface {
	// Key returns the key to store the model against
	Key() string
	// Prefix returns the key prefix used by the module
	Prefix() string
	// Returns the version of the saved model in the registry. Version changes for every update.
	GetVersionID() (string, error)
	//Sets the version of the saved model from the registry. Never to be used by business logic.
	SetVersionID(vid string)
}

// BaseModel implements basic model functionality
type BaseModel struct {
	versionID    string
	isVersionSet bool
}

//SetVersionID Base implementation of set version id
func (b *BaseModel) SetVersionID(vid string) {
	b.isVersionSet = true
	b.versionID = vid
}

//GetVersionID Base implementation of get version id. errors out if called without setting the version id.
func (b *BaseModel) GetVersionID() (string, error) {
	if !b.isVersionSet {
		err := fmt.Errorf("calling getVersionID without setting it")
		return "", err
	}
	return b.versionID, nil
}

// GetBasePrefix returns the prefix
func GetBasePrefix() string {
	return "metro" + "-" + app.GetEnv() + "/"
}
