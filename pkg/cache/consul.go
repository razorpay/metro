package cache

import (
	"context"
	"github.com/razorpay/metro/pkg/cache/mocks"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/opentracing/opentracing-go"
)

// ConsulConfig ...
type ConsulConfig struct {
	api.Config
	Mock bool
}

// ConsulClient ...
type ConsulClient struct {
	client *api.Client
}

// NewConsulClient creates a new consul client
func NewConsulClient(config *ConsulConfig) (ICache, error) {
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

// Get fetches from consul. If key does not exist, empty value is returned.
func (c *ConsulClient) Get(ctx context.Context, key string) ([]byte, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, consulGetSpan)
	defer span.Finish()

	cacheOperationCount.WithLabelValues(env, consulGetOperation).Inc()

	startTime := time.Now()
	defer func() {
		cacheOperationTimeTaken.WithLabelValues(env, consulGetOperation).Observe(time.Now().Sub(startTime).Seconds())
	}()

	kv, _, err := c.client.KV().Get(key, nil)
	if err != nil {
		return nil, err
	}

	var val []byte
	if kv != nil {
		val = kv.Value
	}

	return val, nil
}

// Set calls the PUT api to set a value in consul.
func (c *ConsulClient) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, consulPutSpan)
	defer span.Finish()

	cacheOperationCount.WithLabelValues(env, consulPutOperation).Inc()

	startTime := time.Now()
	defer func() {
		cacheOperationTimeTaken.WithLabelValues(env, consulPutOperation).Observe(time.Now().Sub(startTime).Seconds())
	}()

	_, err := c.client.KV().Put(&api.KVPair{
		Key:   key,
		Value: value,
	}, nil)
	return err
}

// Delete deletes a single key from consul.
func (c *ConsulClient) Delete(ctx context.Context, key string) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, consulDeleteSpan)
	defer span.Finish()

	cacheOperationCount.WithLabelValues(env, consulDeleteOperation).Inc()

	startTime := time.Now()
	defer func() {
		cacheOperationTimeTaken.WithLabelValues(env, consulDeleteOperation).Observe(time.Now().Sub(startTime).Seconds())
	}()

	_, err := c.client.KV().Delete(key, nil)
	if err != nil {
		return err
	}
	return nil
}

// IsAlive checks the health of consul.
func (c *ConsulClient) IsAlive(_ context.Context) (bool, error) {
	health, _, err := c.client.Health().Checks("any", nil)
	return health.AggregatedStatus() == api.HealthPassing, err
}
