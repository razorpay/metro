package cache

import (
	"context"
	"time"
)

// MockConsulClient ...
type MockConsulClient struct {
}

// Get ...
func (m *MockConsulClient) Get(ctx context.Context, key string) ([]byte, error) {
	return []byte(""), nil
}

// Set ...
func (m *MockConsulClient) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return nil
}

// Delete ...
func (m *MockConsulClient) Delete(ctx context.Context, key string) error {
	return nil
}

// IsAlive ...
func (m *MockConsulClient) IsAlive(_ context.Context) (bool, error) {
	return true, nil
}
