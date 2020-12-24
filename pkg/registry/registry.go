package registry

import "time"

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

	// Acquire a lock for a registration_id on a given key and value pair
	Acquire(string, string, string) bool

	// Release a lock for a restration_id on a given key and value pair
	Release(string, string, string) bool

	// Watch on a key for a given registration_id
	Watch(string, string) error

	// Put a key value pair
	Put(key string, value []byte) error
}
