package cache

import (
	"context"
	"net"
	"time"

	"github.com/razorpay/metro/pkg/cache/mocks"

	"github.com/go-redis/redis"
)

// RedisConfig holds all required info for initializing redis driver
type RedisConfig struct {
	Host     string
	Port     string
	Database int32
	Password string
	Mock     bool
}

// RedisClient ...
type RedisClient struct {
	client *redis.Client
}

// NewRedisClient ...
func NewRedisClient(config *RedisConfig) (ICache, error) {
	if config.Mock {
		return &mocks.MockRedisClient{}, nil
	}
	options := &redis.Options{
		Addr:     net.JoinHostPort(config.Host, config.Port),
		Password: config.Password,
		DB:       int(config.Database),
	}
	client := redis.NewClient(options)
	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}
	return &RedisClient{client}, nil
}

// Set ...
func (rc *RedisClient) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	err := rc.client.Set(key, string(value), ttl).Err()
	if err != nil {
		return err
	}

	return nil
}

// Get ...
func (rc *RedisClient) Get(ctx context.Context, key string) ([]byte, error) {
	val, err := rc.client.Get(key).Result()
	if err != nil {
		return nil, err
	}
	return []byte(val), nil
}

// Delete ...
func (rc *RedisClient) Delete(ctx context.Context, key string) error {
	err := rc.client.Del(key).Err()
	return err
}

// MGet ...
func (rc *RedisClient) MGet(keys ...string) ([]interface{}, error) {
	return rc.client.MGet(keys...).Result()
}

// IsAlive ...
func (rc *RedisClient) IsAlive(ctx context.Context) (bool, error) {
	ping, err := rc.client.Ping().Result()
	if err != nil {
		return false, err
	}
	if ping == "PONG" {
		return true, err
	}
	return false, err
}

// Disconnect ... disconnects from the redis server
func (rc *RedisClient) Disconnect() error {
	err := rc.client.Close()
	if err != nil {
		return err
	}
	return nil
}
