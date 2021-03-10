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
	Delete(ctx context.Context, m IModel) error
	DeleteTree(ctx context.Context, key string) error
	Get(ctx context.Context, key string, m IModel) error
	ListKeys(ctx context.Context, prefix string) ([]string, error)
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

// Delete deletes all keys under a model key
func (r BaseRepo) Delete(ctx context.Context, m IModel) error {
	return r.Registry.DeleteTree(m.Key())
}

// DeleteTree deletes all keys under a prefix
func (r BaseRepo) DeleteTree(ctx context.Context, key string) error {
	return r.Registry.DeleteTree(key)
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

// ListKeys populates keys with key list corresponding to prefix
func (r BaseRepo) ListKeys(ctx context.Context, prefix string) ([]string, error) {
	keys, err := r.Registry.ListKeys(ctx, prefix)
	if err != nil {
		return nil, err
	}

	return keys, nil
}
