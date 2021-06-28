package messagebroker

import (
	"os"
	"path/filepath"
	"time"
)

// BrokerConfig holds broker's configuration
type BrokerConfig struct {
	// A list of host/port pairs to use for establishing the initial connection to the broker cluster
	Brokers             []string
	EnableTLS           bool
	UserCertificate     string
	UserKey             string
	CACertificate       string
	CertDir             string
	Version             string
	DebugEnabled        bool
	OperationTimeoutMs  int
	ConnectionTimeoutMs int
	Producer            *ProducerConfig
	Consumer            *ConsumerConfig
	Admin               *AdminConfig
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
	OffsetReset      string
	EnableAutoCommit bool
}

// AdminConfig holds configuration for admin APIs
type AdminConfig struct{}

// ConsumerClientOptions holds client specific configuration for consumer
type ConsumerClientOptions struct {
	// Specify a list of topics to consume messages from
	Topics []string
	// Specify the subscription name for this consumer. Only used for pulsar
	Subscription string
	// A unique string that identifies the consumer group this consumer belongs to.
	GroupID string
	// A unique identifier of the consumer instance provided by the end user.
	// Only non-empty strings are permitted. If set, the consumer is treated as a static member,
	// which means that only one instance with this ID is allowed in the consumer group at any time
	GroupInstanceID string
}

// ProducerClientOptions holds client specific configuration for producer
type ProducerClientOptions struct {
	Topic     string
	TimeoutMs int64
}

// AdminClientOptions holds client specific configuration for admin
type AdminClientOptions struct{}

func getCertFile(certDir, filename string) (string, error) {
	configPath := certDir + filename

	_, err := os.Stat(configPath)
	if err == nil {
		return filepath.Abs(configPath)
	}
	return "", err
}
