package messagebroker

import "time"

// BrokerConfig holds broker's configuration
type BrokerConfig struct {
	Brokers           []string
	EnableTLS         bool
	UserCertificate   string
	UserKey           string
	CACertificate     string
	KafkaVersion      string
	DebugEnabled      bool
	OperationTimeout  int
	ConnectionTimeout int
	Producer          *ProducerConfig
	Consumer          *ConsumerConfig
	Admin             *AdminConfig
}

// ProducerConfig holds producer's configuration'
type ProducerConfig struct {
	RetryBackoff       time.Duration
	Partitioner        string
	MaxRetry           int
	MaxMessages        int
	CompressionEnabled bool
	CompressionType    string
	RetryAck           string
}

// ConsumerConfig holds consumer's configuration
type ConsumerConfig struct {
	SubscriptionType int
	PollInterval     time.Duration
	OffsetReset      string
	EnableAutoCommit bool
}

// AdminConfig holds configuration for admin APIs
type AdminConfig struct{}

// ConsumerClientOptions holds client specific configuration for consumer
type ConsumerClientOptions struct {
	Topic        string
	Subscription string
	GroupID      string
}

// ProducerClientOptions holds client specific configuration for producer
type ProducerClientOptions struct {
	Topic string
}

// AdminClientOptions holds client specific configuration for admin
type AdminClientOptions struct{}
