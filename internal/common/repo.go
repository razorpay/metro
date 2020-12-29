package common

import (
	"context"
	"encoding/json"

	"github.com/razorpay/metro/pkg/registry"
)

// BaseRepo implements base repo methods
type BaseRepo struct {
	Registry registry.IRegistry
}

// Create puts the model in the registry
func (r BaseRepo) Create(ctx context.Context, m IModel) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return r.Registry.Put(m.Key(), b)
}

// Exists to check if the key exists in the registry
func (r BaseRepo) Exists(ctx context.Context, key string) (bool, error) {
	return r.Registry.Exists(key)
}
