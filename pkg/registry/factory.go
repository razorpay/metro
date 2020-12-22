package registry

import "fmt"

const (
	// Consul constant for getting ConsulClient from Registry factory
	Consul = "consul"
)

// NewRegistry initializes the registry instance based on Config
func NewRegistry(config *Config) (IRegistry, error) {
	switch config.Driver {
	case Consul:
		r, err := NewConsulClient(&config.ConsulConfig)
		if err != nil {
			return nil, err
		}
		return r, nil
	default:
		return nil, fmt.Errorf("Unknown Registry Driver: %s", config.Driver)
	}
}
