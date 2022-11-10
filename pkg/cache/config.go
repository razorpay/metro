package cache

// Config holds cache configuration
type Config struct {
	Driver       string
	ConsulConfig ConsulConfig
	RedisConfig  RedisConfig
	Mock         bool
}
