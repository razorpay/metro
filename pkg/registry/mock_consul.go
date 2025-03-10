package registry

import (
	"context"
	"time"
)

type MockConsulClient struct {
}

// Register ...
func (m *MockConsulClient) Register(ctx context.Context, name string, ttl time.Duration) (string, error) {
	return "", nil
}

// IsRegistered ...
func (m *MockConsulClient) IsRegistered(ctx context.Context, sessionID string) bool {
	return true
}

// Renew ...
func (m *MockConsulClient) Renew(ctx context.Context, sessionID string) error {
	return nil
}

// RenewPeriodic ...
func (m *MockConsulClient) RenewPeriodic(ctx context.Context, sessionID string, ttl time.Duration, doneCh <-chan struct{}) error {
	return nil
}

// Deregister ...
func (m *MockConsulClient) Deregister(ctx context.Context, sessionID string) error {
	return nil
}

// Acquire ...
func (m *MockConsulClient) Acquire(ctx context.Context, sessionID string, key string, value []byte) (bool, error) {
	return true, nil
}

// Release ...
func (m *MockConsulClient) Release(ctx context.Context, sessionID string, key string, value string) bool {
	return true
}

// Watch ...
func (m *MockConsulClient) Watch(ctx context.Context, wh *WatchConfig) (IWatcher, error) {
	return nil, nil
}

// Put ...
func (m *MockConsulClient) Put(ctx context.Context, key string, value []byte) (string, error) {
	return "", nil
}

// Get ...
func (m *MockConsulClient) Get(ctx context.Context, key string) (*Pair, error) {
	return nil, nil
}

// List ...
func (m *MockConsulClient) List(ctx context.Context, prefix string) ([]Pair, error) {
	return nil, nil
}

// ListKeys ...
func (m *MockConsulClient) ListKeys(ctx context.Context, key string) ([]string, error) {
	return nil, nil
}

// Exists ...
func (m *MockConsulClient) Exists(ctx context.Context, key string) (bool, error) {
	return true, nil
}

// DeleteTree ...
func (m *MockConsulClient) DeleteTree(ctx context.Context, key string) error {
	return nil
}

// IsAlive ...
func (m *MockConsulClient) IsAlive(_ context.Context) (bool, error) {
	return true, nil
}
