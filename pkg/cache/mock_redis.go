package cache

import (
	"context"
	"time"
)

// MockRedisClient ...
type MockRedisClient struct {
}

func (mc *MockRedisClient) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return nil
}

// Get ...
func (mc *MockRedisClient) Get(ctx context.Context, key string) ([]byte, error) {
	return []byte(""), nil
}

// Delete ...
func (mc *MockRedisClient) Delete(ctx context.Context, key string) error {
	return nil
}

// MGet ...
func (mc *MockRedisClient) MGet(keys ...string) ([]interface{}, error) {
	return nil, nil
}

// IsAlive ...
func (mc *MockRedisClient) IsAlive(ctx context.Context) (bool, error) {
	return true, nil
}

// Disconnect ...
func (mc *MockRedisClient) Disconnect() error {
	return nil
}
