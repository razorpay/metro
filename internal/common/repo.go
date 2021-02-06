package common

import (
	"context"
	"encoding/json"

	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/registry"
)

// IRepo is an interface over common repo functionality
type IRepo interface {
	Create(ctx context.Context, m IModel) error
	Exists(ctx context.Context, key string) (bool, error)
	DeleteTree(ctx context.Context, m IModel) error
	Get(ctx context.Context, key string, m IModel) error
}

// BaseRepo implements base repo methods
type BaseRepo struct {
	Registry registry.IRegistry
}

// Create puts the model in the registry
func (r BaseRepo) Create(ctx context.Context, m IModel) error {
	b, err := json.Marshal(m)
	if err != nil {
		logger.Ctx(ctx).Error("error in json marshal", "msg", err.Error())
		return err
	}
	return r.Registry.Put(m.Key(), b)
}

// Exists to check if the key exists in the registry
func (r BaseRepo) Exists(ctx context.Context, key string) (bool, error) {
	return r.Registry.Exists(key)
}

// DeleteTree deletes all keys under a prefix
func (r BaseRepo) DeleteTree(ctx context.Context, m IModel) error {
	return r.Registry.DeleteTree(m.Key())
}

// Get populates m with value corresponding to key
func (r BaseRepo) Get(ctx context.Context, key string, m IModel) error {
	b, err := r.Registry.Get(ctx, key)
	if err != nil {
		return err
	}
	err = json.Unmarshal(b, m)
	if err != nil {
		return err
	}
	return nil
}
