package queue

import (
	"errors"

	"github.com/razorpay/metro/pkg/worker/queue/broker/redis"
	"github.com/razorpay/metro/pkg/worker/queue/broker/sqs"
)

// IQueue implements a generic interface for message brokers
type IQueue interface {
	Enqueue(message string, delay int64, queueName string) (string, error)
	Dequeue(queueName string) (string, string, error)
	Acknowledge(id string, queueName string) error
}

// Config for initializing a queue broker driver
type Config struct {
	QueueName string
	Driver    string
	SQS       sqs.Config
	Redis     redis.Config
}

// New initializes the queue broker instance based on config
func New(config *Config) (IQueue, error) {
	var (
		q   IQueue
		err error
	)

	switch config.Driver {
	case sqs.Dialect:
		q, err = sqs.New(config.QueueName, &config.SQS)
	case redis.Dialect:
		q, err = redis.New(config.QueueName, &config.Redis)

	default:
		return nil, errors.New("unknown queue broker driver: " + config.Driver)
	}

	if err != nil {
		return nil, err
	}

	return q, nil
}
