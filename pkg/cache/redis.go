package cache

import (
	"context"
	"fmt"
	"time"

	otredis "github.com/opentracing-contrib/goredis"

	"github.com/go-redis/redis"
)

// RedisConfig holds all required info for initializing redis driver
type RedisConfig struct {
	Host     string
	Port     int32
	Database int32
	Password string
}

// RedisCache holds the handler for the redisclient and auxiliary info
type RedisCache struct {
	redisClient otredis.Client
}

// NewRedisClient inits a RedisCache instance
func NewRedisClient(config *RedisConfig) (*RedisCache, error) {
	var client RedisCache

	addr := fmt.Sprintf("%s:%d", config.Host, config.Port)

	options := &redis.UniversalOptions{
		Addrs:    []string{addr},
		Password: config.Password,
		DB:       int(config.Database),
	}

	client.redisClient = otredis.Wrap(redis.NewUniversalClient(options))

	_, err := client.redisClient.Ping().Result()
	if err != nil {
		return nil, err
	}

	return &client, nil
}

// Set -
func (rc *RedisCache) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	err := rc.redisClient.WithContext(ctx).Set(key, value, ttl).Err()
	if err != nil {
		return err
	}

	return nil
}

// Get -
func (rc *RedisCache) Get(ctx context.Context, key string) (string, error) {
	val, err := rc.redisClient.WithContext(ctx).Get(key).Result()
	if err != nil {
		return "", err
	}
	return val, nil
}

// Delete -
func (rc *RedisCache) Delete(ctx context.Context, key string) error {
	err := rc.redisClient.WithContext(ctx).Del(key).Err()
	return err
}

//Disconnect ... disconnects from the redis server
func (rc *RedisCache) Disconnect() error {
	err := rc.redisClient.Close()
	if err != nil {
		return err
	}
	return nil
}
