package registry

import (
	"context"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
)

// ConsulClient ...
type ConsulClient struct {
	ctx      context.Context
	client   *api.Client
	plans    []*watch.Plan
	watchers []*ConsulWatchHandler
}

// ConsulConfig for ConsulClient
type ConsulConfig struct {
	api.Config
}

var once sync.Once

// NewConsulClient creates a new consul client
func NewConsulClient(ctx context.Context, config *ConsulConfig) (IRegistry, error) {
	var c *ConsulClient
	var err error

	once.Do(func() {
		var client *api.Client

		client, err = api.NewClient(&config.Config)

		if err == nil {
			c = &ConsulClient{
				ctx:    ctx,
				client: client,
			}
		}
	})

	return c, err
}

// Register is used for node registration which creates a session to consul
func (c *ConsulClient) Register(name string, ttl time.Duration) (string, error) {
	sessionID, _, err := c.client.Session().Create(&api.SessionEntry{
		Name:      name,
		TTL:       ttl.String(),
		LockDelay: 2,
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

// Deregister node, destroy consul session
func (c *ConsulClient) Deregister(sessionID string) error {
	_, err := c.client.Session().Destroy(sessionID, nil)

	return err
}

// Acquire a lock
func (c *ConsulClient) Acquire(sessionID string, key string, value string) bool {
	isAcquired, _, err := c.client.KV().Acquire(&api.KVPair{
		Key:     key,
		Value:   []byte(value),
		Session: sessionID,
	}, nil)

	if err != nil {
		return false
	}

	return isAcquired
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

// Watch watches a key
func (c *ConsulClient) Watch(watchType string, key string, handler HandlerFunc) error {
	plan, err := watch.Parse(map[string]interface{}{
		"type":   watchType,
		"prefix": key,
	})

	if err != nil {
		return err
	}

	wh := NewConsulWatchHandler(handler)

	// add the plan to list so that we have an updated reference to all the plans
	// this will used while terminating the application gracefully
	c.plans = append(c.plans, plan)
	c.watchers = append(c.watchers, wh)

	plan.Handler = wh.Handler

	return plan.RunWithClientAndLogger(c.client, nil)
}

// Put a key value pair
func (c *ConsulClient) Put(key string, value []byte) error {
	_, err := c.client.KV().Put(&api.KVPair{
		Key:   key,
		Value: value,
	}, nil)
	return err
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
