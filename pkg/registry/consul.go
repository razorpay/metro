package registry

import (
	"sync"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
)

// ConsulClient ...
type ConsulClient struct {
	client *api.Client
}

// ConsulConfig for ConsulClient
type ConsulConfig struct {
	api.Config
}

var once sync.Once

// NewConsulClient creates a new consul client
func NewConsulClient(config ConsulConfig) (*ConsulClient, error) {
	var c *ConsulClient
	var err error

	once.Do(func() {
		var client *api.Client

		client, err = api.NewClient(&config.Config)

		if err == nil {
			c = &ConsulClient{
				client: client,
			}
		}
	})

	return c, err
}

// CreateSession creates a session to consul
func (c *ConsulClient) CreateSession(name string) (string, error) {
	sessionID, _, err := c.client.Session().Create(&api.SessionEntry{
		Name: name,
	}, nil)

	if err != nil {
		return "", err
	}
	return sessionID, nil
}

// RenewPeriodic renews consul session to be used in a long
// running goroutine to ensure a session stays valid
func (c *ConsulClient) RenewPeriodic(sessionID string, doneChan chan struct{}) error {
	return c.client.Session().RenewPeriodic(
		"10s",
		sessionID,
		nil,
		doneChan,
	)
}

// Destroy consul session
func (c *ConsulClient) Destroy(sessionID string) error {
	_, err := c.client.Session().Destroy(sessionID, nil)

	return err
}

// Acquire a lock
func (c *ConsulClient) Acquire(sessionID string, key string, value string) (bool, error) {
	isAcquired, _, err := c.client.KV().Acquire(&api.KVPair{
		Key:     key,
		Value:   []byte(value),
		Session: sessionID,
	}, nil)

	if err != nil {
		return false, err
	}

	return isAcquired, nil
}

// Release a lock
func (c *ConsulClient) Release(sessionID string, key string, value string) (bool, error) {
	isReleased, _, err := c.client.KV().Release(&api.KVPair{
		Key:     key,
		Value:   []byte(value),
		Session: sessionID,
	}, nil)

	if err != nil {
		return false, err
	}

	return isReleased, nil
}

// WatchKey watches a key
func (c *ConsulClient) WatchKey(key string) error {
	plan, err := watch.Parse(map[string]interface{}{
		"type":   "keyprefix",
		"prefix": key,
	})

	if err != nil {
		return err
	}

	plan.Handler = c.handler

	err = plan.RunWithClientAndLogger(c.client, nil)
	if err != nil {
		return err
	}
	return nil
}

func (c *ConsulClient) handler(_ uint64, result interface{}) {
}
