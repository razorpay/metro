package registry

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/razorpay/metro/pkg/registry/mocks"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"

	"github.com/razorpay/metro/pkg/logger"
)

// ConsulClient ...
type ConsulClient struct {
	client *api.Client
}

// ConsulConfig for ConsulClient
type ConsulConfig struct {
	api.Config
	Mock bool
}

// NewConsulClient creates a new consul client
func NewConsulClient(config *ConsulConfig) (IRegistry, error) {
	if config.Mock {
		return &mocks.MockConsulClient{}, nil
	}

	client, err := api.NewClient(&config.Config)

	if err != nil {
		return nil, err
	}

	return &ConsulClient{
		client: client,
	}, nil
}

// Register is used for node registration which creates a session to consul
func (c *ConsulClient) Register(ctx context.Context, name string, ttl time.Duration) (string, error) {
	registryOperationCount.WithLabelValues(env, "Register").Inc()

	startTime := time.Now()
	defer func() {
		registryOperationTimeTaken.WithLabelValues(env, "Register").Observe(time.Now().Sub(startTime).Seconds())
	}()

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
func (c *ConsulClient) IsRegistered(ctx context.Context, sessionID string) bool {
	registryOperationCount.WithLabelValues(env, "IsRegistered").Inc()

	startTime := time.Now()
	defer func() {
		registryOperationTimeTaken.WithLabelValues(env, "IsRegistered").Observe(time.Now().Sub(startTime).Seconds())
	}()

	session, _, err := c.client.Session().Info(sessionID, nil)
	if err != nil {
		logger.Ctx(ctx).Errorw("failed to get session info", "error", err.Error())
		return false
	}

	return session != nil
}

// Renew renews consul session
func (c *ConsulClient) Renew(ctx context.Context, sessionID string) error {
	registryOperationCount.WithLabelValues(env, "Renew").Inc()

	startTime := time.Now()
	defer func() {
		registryOperationTimeTaken.WithLabelValues(env, "Renew").Observe(time.Now().Sub(startTime).Seconds())
	}()

	sessionEntry, _, err := c.client.Session().Renew(sessionID, nil)

	// if err is nil, it means client failed to renew session
	if err != nil {
		logger.Ctx(ctx).Errorw("failed to renew consul session", "error", err.Error())
		return err
	}

	// if err is nil and sessionEntry is nil, session already expired before renew
	if sessionEntry == nil {
		return api.ErrSessionExpired
	}

	// session successfully renewed
	return nil
}

// RenewPeriodic renews consul session periodically until doneCh signals done
func (c *ConsulClient) RenewPeriodic(ctx context.Context, sessionID string, ttl time.Duration, doneCh <-chan struct{}) error {
	registryOperationCount.WithLabelValues(env, "RenewPeriodic").Inc()

	startTime := time.Now()
	defer func() {
		registryOperationTimeTaken.WithLabelValues(env, "RenewPeriodic").Observe(time.Now().Sub(startTime).Seconds())
	}()

	return c.client.Session().RenewPeriodic(ttl.String(), sessionID, nil, doneCh)
}

// Deregister node, destroy consul session
func (c *ConsulClient) Deregister(ctx context.Context, sessionID string) error {
	registryOperationCount.WithLabelValues(env, "Deregister").Inc()

	startTime := time.Now()
	defer func() {
		registryOperationTimeTaken.WithLabelValues(env, "Deregister").Observe(time.Now().Sub(startTime).Seconds())
	}()

	_, err := c.client.Session().Destroy(sessionID, nil)
	if err != nil {
		logger.Ctx(ctx).Errorw("failed to destroy consul session", "ID", sessionID, "error", err.Error())
	}

	return err
}

// Acquire a lock
func (c *ConsulClient) Acquire(ctx context.Context, sessionID string, key string, value []byte) (bool, error) {
	registryOperationCount.WithLabelValues(env, "Acquire").Inc()

	startTime := time.Now()
	defer func() {
		registryOperationTimeTaken.WithLabelValues(env, "Acquire").Observe(time.Now().Sub(startTime).Seconds())
	}()

	isAcquired, _, err := c.client.KV().Acquire(&api.KVPair{
		Key:     key,
		Value:   value,
		Session: sessionID,
	}, nil)

	if err != nil {
		logger.Ctx(ctx).Errorw("error in consul acquire call", "ID", sessionID, "key", key, "error", err.Error())
		return false, err
	}

	return isAcquired, nil
}

// Release a lock
func (c *ConsulClient) Release(ctx context.Context, sessionID string, key string, value string) bool {
	registryOperationCount.WithLabelValues(env, "Release").Inc()

	startTime := time.Now()
	defer func() {
		registryOperationTimeTaken.WithLabelValues(env, "Release").Observe(time.Now().Sub(startTime).Seconds())
	}()

	isReleased, _, err := c.client.KV().Release(&api.KVPair{
		Key:     key,
		Value:   []byte(value),
		Session: sessionID,
	}, nil)

	if err != nil {
		logger.Ctx(ctx).Errorw("error in consul release", "ID", sessionID, "key", key, "error", err.Error())
		return false
	}

	return isReleased
}

// Watch watches a key and returns a watcher instance, this instance should be used to terminate the watch
func (c *ConsulClient) Watch(ctx context.Context, wh *WatchConfig) (IWatcher, error) {
	registryOperationCount.WithLabelValues(env, "Watch").Inc()

	startTime := time.Now()
	defer func() {
		registryOperationTimeTaken.WithLabelValues(env, "Watch").Observe(time.Now().Sub(startTime).Seconds())
	}()

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
func (c *ConsulClient) Put(ctx context.Context, key string, value []byte) (string, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "ConsulClient.Put")
	defer span.Finish()

	registryOperationCount.WithLabelValues(env, "Put").Inc()

	startTime := time.Now()
	defer func() {
		registryOperationTimeTaken.WithLabelValues(env, "Put").Observe(time.Now().Sub(startTime).Seconds())
	}()

	// Using transaction because a simple put does not fetch the modifyindex
	putTxn := api.TxnOps{&api.TxnOp{KV: &api.KVTxnOp{Verb: api.KVSet, Key: key, Value: value}}}
	ok, resp, _, err := c.client.Txn().Txn(putTxn, nil)
	if !ok {
		if err != nil {
			return "", err
		}
		if len(resp.Errors) == 0 {
			// scenario not possible
			err = fmt.Errorf("could not execute put")
		} else {
			err = fmt.Errorf(resp.Errors[0].What)
		}
		return "", err
	}

	if len(resp.Results) == 0 || resp.Results[0].KV == nil {
		// scenario not possible
		err = fmt.Errorf("error in consul transaction: received empty result list")
		return "", err
	}

	return strconv.FormatUint(resp.Results[0].KV.ModifyIndex, 10), err
}

// Get returns a value for a key
func (c *ConsulClient) Get(ctx context.Context, key string) (*Pair, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "ConsulClient.Get")
	defer span.Finish()

	registryOperationCount.WithLabelValues(env, "Get").Inc()

	startTime := time.Now()
	defer func() {
		registryOperationTimeTaken.WithLabelValues(env, "Get").Observe(time.Now().Sub(startTime).Seconds())
	}()

	kv, _, err := c.client.KV().Get(key, nil)
	if err != nil {
		return nil, err
	}
	if kv == nil {
		return nil, errors.New("key not found")
	}
	return &Pair{Key: key, Value: kv.Value, Version: strconv.FormatUint(kv.ModifyIndex, 10)}, nil
}

// List returns a slice of pairs for a key prefix
func (c *ConsulClient) List(ctx context.Context, prefix string) ([]Pair, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "ConsulClient.List")
	defer span.Finish()

	registryOperationCount.WithLabelValues(env, "List").Inc()

	startTime := time.Now()
	defer func() {
		registryOperationTimeTaken.WithLabelValues(env, "List").Observe(time.Now().Sub(startTime).Seconds())
	}()

	kvs, _, err := c.client.KV().List(prefix, nil)
	if err != nil {
		return nil, err
	}

	pairs := []Pair{}

	for _, kv := range kvs {
		pairs = append(pairs, Pair{
			Key:     kv.Key,
			Value:   kv.Value,
			Version: strconv.FormatUint(kv.ModifyIndex, 10),
		})
	}

	return pairs, nil
}

// ListKeys returns a value for a key
func (c *ConsulClient) ListKeys(ctx context.Context, key string) ([]string, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "ConsulClient.ListKeys")
	defer span.Finish()

	registryOperationCount.WithLabelValues(env, "ListKeys").Inc()

	startTime := time.Now()
	defer func() {
		registryOperationTimeTaken.WithLabelValues(env, "ListKeys").Observe(time.Now().Sub(startTime).Seconds())
	}()

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
func (c *ConsulClient) Exists(ctx context.Context, key string) (bool, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "ConsulClient.Exists")
	defer span.Finish()

	registryOperationCount.WithLabelValues(env, "Exists").Inc()

	startTime := time.Now()
	defer func() {
		registryOperationTimeTaken.WithLabelValues(env, "Exists").Observe(time.Now().Sub(startTime).Seconds())
	}()

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
func (c *ConsulClient) DeleteTree(ctx context.Context, key string) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "ConsulClient.DeleteTree")
	defer span.Finish()

	registryOperationCount.WithLabelValues(env, "DeleteTree").Inc()

	startTime := time.Now()
	defer func() {
		registryOperationTimeTaken.WithLabelValues(env, "DeleteTree").Observe(time.Now().Sub(startTime).Seconds())
	}()

	_, err := c.client.KV().DeleteTree(key, nil)
	if err != nil {
		return err
	}
	return nil
}

// IsAlive checks the health of the consul
func (c *ConsulClient) IsAlive(_ context.Context) (bool, error) {
	health, _, err := c.client.Health().Checks("any", nil)
	return health.AggregatedStatus() == api.HealthPassing, err
}
