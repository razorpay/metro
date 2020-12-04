package messagebroker

import "time"

type BrokerConfig struct {
	Producer ProducerConfig
	Consumer ConsumerConfig
	Admin    AdminConfig
}

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

type ConsumerConfig struct {
	BrokerList   string
	GroupID      string
	Topic        string
	PollInterval time.Duration
}

type AdminConfig struct{}
