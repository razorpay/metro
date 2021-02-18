package registry

import (
	"context"
	"time"
)

// Pair is the registry struct returned to watch handler
type Pair struct {
	Key       string
	Value     []byte
	SessionID string
}

// IRegistry implements a generic interface for service discovery
//go:generate go run -mod=mod github.com/golang/mock/mockgen -build_flags=-mod=mod -destination=mocks/mock_registry.go -package=mocks . IRegistry
type IRegistry interface {
	// Register a node with the Registry with a given name
	// Returns a Registration id or error
	Register(string, time.Duration) (string, error)

	// Deregister a node which was registered with a id
	// Returns error on failure
	Deregister(string) error

	// IsRegistered checks is node with registration_id is registred with registry
	IsRegistered(string) bool

	// Renew a regestration using registration_id
	Renew(string) error

	// RenewPeriodic renews a registration id periodically based on TTL
	RenewPeriodic(string, time.Duration, <-chan struct{}) error

	// Acquire a lock for a registration_id on a given key and value pair
	Acquire(string, string, string) (bool, error)

	// Release a lock for a restration_id on a given key and value pair
	Release(string, string, string) bool

	// Watch on a key/keyprefix in registry
	Watch(ctx context.Context, wh *WatchConfig) (IWatcher, error)

	// Put a key value pair
	Put(key string, value []byte) error

	// Exists checks the existence of a key
	Exists(key string) (bool, error)

	// DeleteTree deletes all keys under a prefix
	DeleteTree(key string) error
}
