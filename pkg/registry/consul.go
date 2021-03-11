package registry

import (
	"context"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
)

// ConsulClient ...
type ConsulClient struct {
	ctx    context.Context
	client *api.Client
}

// ConsulConfig for ConsulClient
type ConsulConfig struct {
	api.Config
}

// NewConsulClient creates a new consul client
func NewConsulClient(ctx context.Context, config *ConsulConfig) (IRegistry, error) {
	client, err := api.NewClient(&config.Config)

	if err != nil {
		return nil, err
	}

	return &ConsulClient{
		ctx:    ctx,
		client: client,
	}, nil
}

// Register is used for node registration which creates a session to consul
func (c *ConsulClient) Register(name string, ttl time.Duration) (string, error) {
	sessionID, _, err := c.client.Session().Create(&api.SessionEntry{
		Name:      name,
		TTL:       ttl.String(),
		LockDelay: 2,
		Behavior:  api.SessionBehaviorDelete,
	}, nil)

	if err != nil {
		return "", err
	}
	return sessionID, nil
}

// IsRegistered is used checking the registration status
func (c *ConsulClient) IsRegistered(sessionID string) bool {
	session, _, _ := c.client.Session().Info(sessionID, nil)

	return session != nil
}

// Renew renews consul session
func (c *ConsulClient) Renew(sessionID string) error {
	_, _, err := c.client.Session().Renew(sessionID, nil)

	return err
}

// RenewPeriodic renews consul session periodically until doneCh signals done
func (c *ConsulClient) RenewPeriodic(sessionID string, ttl time.Duration, doneCh <-chan struct{}) error {
	return c.client.Session().RenewPeriodic(ttl.String(), sessionID, nil, doneCh)
}

// Deregister node, destroy consul session
func (c *ConsulClient) Deregister(sessionID string) error {
	_, err := c.client.Session().Destroy(sessionID, nil)

	return err
}

// Acquire a lock
func (c *ConsulClient) Acquire(sessionID string, key string, value []byte) (bool, error) {
	isAcquired, _, err := c.client.KV().Acquire(&api.KVPair{
		Key:     key,
		Value:   value,
		Session: sessionID,
	}, nil)

	if err != nil {
		return false, err
	}

	return isAcquired, nil
}

// Release a lock
func (c *ConsulClient) Release(sessionID string, key string, value string) bool {
	isReleased, _, err := c.client.KV().Release(&api.KVPair{
		Key:     key,
		Value:   []byte(value),
		Session: sessionID,
	}, nil)

	if err != nil {
		return false
	}

	return isReleased
}

// Watch watches a key and returns a watcher instance, this instance should be used to terminate the watch
func (c *ConsulClient) Watch(ctx context.Context, wh *WatchConfig) (IWatcher, error) {
	params := map[string]interface{}{}

	if wh.WatchType == "key" {
		params["type"] = wh.WatchType
		params["key"] = wh.WatchPath
	} else if wh.WatchType == "keyprefix" {
		params["type"] = wh.WatchType
		params["prefix"] = wh.WatchPath
	}

	plan, err := watch.Parse(params)
	if err != nil {
		return nil, err
	}
	cwh := NewConsulWatcher(ctx, wh, plan, c.client)

	return cwh, nil
}

// Put a key value pair
func (c *ConsulClient) Put(key string, value []byte) error {
	_, err := c.client.KV().Put(&api.KVPair{
		Key:   key,
		Value: value,
	}, nil)
	return err
}

// Get returns a value for a key
func (c *ConsulClient) Get(ctx context.Context, key string) ([]byte, error) {
	kv, _, err := c.client.KV().Get(key, nil)
	if err != nil {
		return nil, err
	}
	return kv.Value, nil
}

// List returns a slice of pairs for a key prefix
func (c *ConsulClient) List(ctx context.Context, prefix string) ([]Pair, error) {
	kvs, _, err := c.client.KV().List(prefix, nil)
	if err != nil {
		return nil, err
	}

	pairs := []Pair{}

	for _, kv := range kvs {
		pairs = append(pairs, Pair{
			Key:   kv.Key,
			Value: kv.Value,
		})
	}

	return pairs, nil
}

// ListKeys returns a value for a key
func (c *ConsulClient) ListKeys(ctx context.Context, key string) ([]string, error) {
	kv, _, err := c.client.KV().List(key, nil)
	if err != nil {
		return nil, err
	}

	keys := []string{}

	for _, pair := range kv {
		keys = append(keys, pair.Key)
	}

	return keys, nil
}

// Exists checks the existence of a key
func (c *ConsulClient) Exists(key string) (bool, error) {
	kvp, _, err := c.client.KV().Get(key, nil)
	if err != nil {
		return false, err
	}
	if kvp == nil {
		return false, nil
	}
	return true, nil
}

// DeleteTree deletes all keys under a prefix
func (c *ConsulClient) DeleteTree(key string) error {
	_, err := c.client.KV().DeleteTree(key, nil)
	if err != nil {
		return err
	}
	return nil
}
