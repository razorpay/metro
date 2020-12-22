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
