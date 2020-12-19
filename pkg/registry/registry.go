package registry

// Registry implements a generic interface for service discovery
type Registry interface {
	// Register a service with the Registry with a given name
	// Returns a Registration id or error
	Register(string) (string, error)

	// Deregister a service which was registred with a id
	// Returns error on failure
	Deregister(string) error

	// Acquire a lock for a registration_id on a given key and value pair
	Acquire(string, string, string) error

	// Renew a acquired lock
	Renew(string) error

	// Release a lock for a restration_id on a given key and value pair
	Release(string, string, string) error

	// Watch on a key for a given registration_id
	Watch(string, string) error

	// Put a key value pair
	Put(key string, value []byte) error
}
