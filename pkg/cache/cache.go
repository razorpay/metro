package cache

import (
	"context"
	"fmt"
	"time"
)

// Cache implements a generic interface for cache clients
type Cache interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key string, value string, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
}

// Config for initializing a cache instance
type Config struct {
	Driver         string
	MaxConnections int32
	ProcessTimeout int32
	Redis          RedisConfig
	InMemory       InMemoryConfig
}

var (
	C Cache
)

// NewCache initializes the cache instance based on Config
func NewCache(config *Config) error {
	var err error
	switch config.Driver {
	case "redis":
		C, err = NewRedisClient(&config.Redis)
		if err != nil {
			return err
		}
	case "memory":
		C, err = NewCacheInstance(&config.InMemory)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("Unknown QueueBroker Driver: %s", config.Driver)
	}
	return err
}
