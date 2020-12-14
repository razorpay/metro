package registry

import "fmt"

const (
	Consul = "consul"
)

// NewRegistry initializes the registry instance based on Config
func NewRegistry(config *Config) (Registry, error) {
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
