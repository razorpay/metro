package common

const (
	basePrefix = "registry/"
)

// IModel interface which all models should implement
type IModel interface {
	// Key returns the key to store the model against
	Key() string
	// Prefix returns the key prefix used by the module
	Prefix() string
}

// BaseModel implements basic model functionality
type BaseModel struct{}

// Prefix returns the global base prefix
func (m *BaseModel) Prefix() string {
	return basePrefix
}
