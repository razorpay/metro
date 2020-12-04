package registry

import (
	"sync"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
)

type ConsulClient struct {
	client *api.Client
}

type ConsulConfig struct {
	api.Config
}

var once sync.Once

/**
 * Create a new Consul singleton client
 */
func NewClient(config ConsulConfig) (*ConsulClient, error) {
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

func (c *ConsulClient) CreateSession(name string) (string, error) {
	sessionId, _, err := c.client.Session().Create(&api.SessionEntry{
		Name: name,
	}, nil)

	if err != nil {
		return "", err
	}
	return sessionId, nil
}

func (c *ConsulClient) RenewPeriodic(sessionId string, doneChan chan struct{}) error {
	return c.client.Session().RenewPeriodic(
		"10s",
		sessionId,
		nil,
		doneChan,
	)
}

func (c *ConsulClient) Destroy(sessionId string) error {
	_, err := c.client.Session().Destroy(sessionId, nil)

	return err
}

func (c *ConsulClient) Acquire(sessionId string, key string, value string) (bool, error) {
	isAcquired, _, err := c.client.KV().Acquire(&api.KVPair{
		Key:     key,
		Value:   []byte(value),
		Session: sessionId,
	}, nil)

	if err != nil {
		return false, err
	}

	return isAcquired, nil
}

func (c *ConsulClient) Release(sessionId string, key string, value string) (bool, error) {
	isReleased, _, err := c.client.KV().Release(&api.KVPair{
		Key:     key,
		Value:   []byte(value),
		Session: sessionId,
	}, nil)

	if err != nil {
		return false, err
	}

	return isReleased, nil
}

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
