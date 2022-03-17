package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis"
)

// RedisConfig holds all required info for initializing redis driver
type RedisConfig struct {
	Host     string
	Port     int32
	Database int32
	Password string
}

type RedisClient struct {
	client *redis.Client
}

func NewRedisClient(config *RedisConfig) (*RedisClient, error) {
	options := &redis.Options{
		Addr:     fmt.Sprintf("%s:%d", config.Host, config.Port),
		Password: config.Password,
		DB:       int(config.Database),
	}
	client := redis.NewClient(options)
	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}
	return &RedisClient{client}, nil
}

// Set -
func (rc *RedisClient) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	err := rc.client.Set(key, string(value), ttl).Err()
	if err != nil {
		return err
	}

	return nil
}

// Get -
func (rc *RedisClient) Get(ctx context.Context, key string) ([]byte, error) {
	val, err := rc.client.Get(key).Result()
	if err != nil {
		return nil, err
	}
	return []byte(val), nil
}

// Delete -
func (rc *RedisClient) Delete(ctx context.Context, key string) error {
	err := rc.client.Del(key).Err()
	return err
}

func (rc *RedisClient) MGet(keys ...string) ([]interface{}, error) {
	return rc.client.MGet(keys...).Result()
}

func (rc *RedisClient) IsAlive(ctx context.Context) (bool, error) {
	ping, err := rc.client.Ping().Result()
	if err != nil {
		return false, err
	}
	if ping == "PONG" {
		return true, err
	}
	return false, err
}

//Disconnect ... disconnects from the redis server
func (rc *RedisClient) Disconnect() error {
	err := rc.client.Close()
	if err != nil {
		return err
	}
	return nil
}
