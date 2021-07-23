package registry

import (
	"context"
	"fmt"
	"time"
)

// Pair is the registry struct returned to watch handler
type Pair struct {
	Key       string
	Value     []byte
	VersionID string
	SessionID string
}

func (pair *Pair) String() string {
	return fmt.Sprintf("key: %s, value: %s, sessionID: %s, versionID:%s", pair.Key, string(pair.Value), pair.SessionID, pair.VersionID)
}

// IRegistry implements a generic interface for service discovery
type IRegistry interface {
	// Register a node with the Registry with a given name
	// Returns a Registration id or error
	Register(ctx context.Context, name string, ttl time.Duration) (string, error)

	// Deregister a node which was registered with a id
	// Returns error on failure
	Deregister(ctx context.Context, id string) error

	// IsRegistered checks is node with registration_id is registred with registry
	IsRegistered(ctx context.Context, id string) bool

	// Renew a registration using registration_id
	Renew(ctx context.Context, id string) error

	// RenewPeriodic renews a registration id periodically based on TTL
	RenewPeriodic(ctx context.Context, id string, ttl time.Duration, doneCh <-chan struct{}) error

	// Acquire a lock for a registration_id on a given key and value pair
	Acquire(ctx context.Context, id string, key string, value []byte) (bool, error)

	// Release a lock for a registration_id on a given key and value pair
	Release(ctx context.Context, id string, key string, value string) bool

	// Watch on a key/keyprefix in registry
	Watch(ctx context.Context, wh *WatchConfig) (IWatcher, error)

	// Put a key value pair
	Put(ctx context.Context, key string, value []byte) (string, error)

	// Get returns a value for a key
	Get(ctx context.Context, key string) (Pair, error)

	// List returns a keys with matching key prefix
	ListKeys(ctx context.Context, prefix string) ([]string, error)

	// List returns a slice of pairs with matching key prefix
	List(ctx context.Context, prefix string) ([]Pair, error)

	// Exists checks the existence of a key
	Exists(ctx context.Context, key string) (bool, error)

	// DeleteTree deletes all keys under a prefix
	DeleteTree(ctx context.Context, key string) error

	// IsAlive checks the health of the registry
	IsAlive(context.Context) (bool, error)
}
