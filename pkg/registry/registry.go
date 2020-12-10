package registry

// Config holds registry configuration
type Config struct {
	Name         string
	Driver       string
	ConsulConfig ConsulConfig
}
