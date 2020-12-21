package messagebroker

import "time"

// BrokerConfig holds broker's configuration
type BrokerConfig struct {
	Producer *ProducerConfig
	Consumer *ConsumerConfig
	Admin    *AdminConfig
}

// ProducerConfig holds producer's configuration'
type ProducerConfig struct {
	RetryBackoff       time.Duration
	Partitioner        string
	MaxRetry           int
	MaxMessages        int
	CompressionEnabled bool
	CompressionType    string
	Brokers            []string
	EnableTLS          bool
	UserCertificate    string
	UserKey            string
	CACertificate      string
	KafkaVersion       string
	DebugEnabled       bool
	RetryAck           string
}

// ConsumerConfig holds consumer's configuration
type ConsumerConfig struct {
	BrokerList   string
	GroupID      string
	Topic        string
	PollInterval time.Duration
}

// AdminConfig holds configuration for admin APIs
type AdminConfig struct{}
