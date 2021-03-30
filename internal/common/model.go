package common

import "github.com/razorpay/metro/internal/app"

// IModel interface which all models should implement
type IModel interface {
	// Key returns the key to store the model against
	Key() string
	// Prefix returns the key prefix used by the module
	Prefix() string
}

// BaseModel implements basic model functionality
type BaseModel struct{}

// GetBasePrefix returns the prefix
func GetBasePrefix() string {
	return "metro" + "-" + app.GetEnv() + "/"
}
