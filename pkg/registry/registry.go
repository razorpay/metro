package registry

import "time"

// Registry implements a generic interface for service discovery
type Registry interface {
	// Register a node with the Registry with a given name
	// Returns a Registration id or error
	Register(string, time.Duration) (string, error)

	// Deregister a service which was registred with a id
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
