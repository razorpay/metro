package cache

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/patrickmn/go-cache"
)

type InMemoryConfig struct {
	defaultExpiration int32
	cleanupInterval   int32
}

var (
	imc     *InMemoryCache
	memOnce sync.Once
)

// InMemoryCache holds an instance to the memory back cache driver
type InMemoryCache struct {
	mem *cache.Cache
}

// NewCacheInstance returns c (i.e. cache instance).
func NewCacheInstance(conf *InMemoryConfig) (*InMemoryCache, error) {
	memOnce.Do(func() {
		defaultExpiration := time.Duration(conf.defaultExpiration) * time.Minute
		cleanupInterval := time.Duration(conf.cleanupInterval) * time.Minute
		instance := cache.New(defaultExpiration, cleanupInterval)
		imc = &InMemoryCache{
			mem: instance,
		}
	})

	return imc, nil
}

// Set - Refer cache.Cache.Set
func (imc *InMemoryCache) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	imc.mem.Set(key, value, ttl)
	return nil
}

// Get - Refer cache.Cache.Get
func (imc *InMemoryCache) Get(ctx context.Context, key string) (string, error) {
	val, found := imc.mem.Get(key)
	if !found {
		return "", fmt.Errorf("Key provided does not exist.")
	}
	return val.(string), nil
}

// Delete - Refer cache.Cache.Delete
func (imc *InMemoryCache) Delete(ctx context.Context, key string) error {
	_, found := imc.mem.Get(key)
	if found {
		imc.mem.Delete(key)
	}
	return nil
}

// Flush flushes all keys from cache. This is available only for memory cache
// and is helpful in tests.
func (imc *InMemoryCache) Flush() {
	imc.mem.Flush()
}
