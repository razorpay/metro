package messagebroker

import "time"

// TODO : re-structure this, duplicates!!

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
	Topic              string
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
	GroupID          string
	Subscription     string
	Topic            string
	SubscriptionType int
	PollInterval     time.Duration
}

// AdminConfig holds configuration for admin APIs
type AdminConfig struct{}
