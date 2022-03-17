package cache

import (
	"fmt"
)

const (
	// Consul constant for getting ConsulClient from Cache factory
	Consul = "consul"
	// Redis constant for getting Redis cache client
	Redis = "redis"
)

// NewCache initializes the cache instance based on Config
func NewCache(config *Config) (ICache, error) {
	switch config.Driver {
	case Consul:
		r, err := NewConsulClient(&config.ConsulConfig)
		if err != nil {
			return nil, err
		}
		return r, nil
	case Redis:
		r, err := NewRedisClient(&config.RedisConfig)
		if err != nil {
			return nil, err
		}
		return r, nil
	default:
		return nil, fmt.Errorf("Unknown Cache Driver: %s", config.Driver)
	}
}
