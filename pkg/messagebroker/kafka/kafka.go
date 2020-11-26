package kafka

import (
	"context"
	"errors"
	"strings"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

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

type ConsumerConfig struct{}

type AdminConfig struct{}

type BrokerConfig struct {
	Producer ProducerConfig
	Consumer ConsumerConfig
	Admin    AdminConfig
}

type KafkaBroker struct {
	Producer *kafka.Producer
	Consumer *kafka.Consumer
	Admin    *kafka.AdminClient
	ctx      context.Context
	bConfig  *BrokerConfig
}

func NewKafkaBroker(ctx context.Context, bConfig *BrokerConfig) (*KafkaBroker, error) {

	if ctx == nil {
		return nil, errors.New("Empty Context in constructor")
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": strings.Join(bConfig.Producer.Brokers, ",")})
	if err != nil {
		panic(err)
	}

	if err != nil {
		panic(err)
	}

	if ctx == nil {
		ctx = context.Background()
	}

	return &KafkaBroker{Producer: producer, ctx: ctx, bConfig: bConfig}, nil
}
