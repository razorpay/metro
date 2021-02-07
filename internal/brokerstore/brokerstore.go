package brokerstore

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/razorpay/metro/pkg/messagebroker"
)

// Key ...
type Key struct {
	partition int    // topic partition
	topic     string // topic name
}

// NewKey creates a new key for broker map
func NewKey(topic string, partition int) *Key {
	return &Key{
		partition: partition,
		topic:     topic,
	}
}

// Prefix returns only the topic name to run a match all query
func (key *Key) Prefix() string {
	return key.topic
}

func (key *Key) String() string {
	return fmt.Sprintf("%v-%v", key.topic, key.partition)
}

// BrokerStore holds broker clients
type BrokerStore struct {
	// stores active producer clients for a key.
	producerMap sync.Map

	// stores active consumer clients for a key
	consumerMap sync.Map

	// stores an active admin client
	admin messagebroker.Admin

	// the broker variant
	variant string

	// the broker config
	bConfig *messagebroker.BrokerConfig
}

// IBrokerStore ...
type IBrokerStore interface {

	// GetOrCreateConsumer returns for an existing consumer instance, if available returns that else creates as new instance
	GetOrCreateConsumer(ctx context.Context, op messagebroker.ConsumerClientOptions) (messagebroker.Consumer, error)

	// GetOrCreateProducer returns for an existing producer instance, if available returns that else creates as new instance
	GetOrCreateProducer(ctx context.Context, op messagebroker.ProducerClientOptions) (messagebroker.Producer, error)

	// GetOrCreateAdmin returns for an existing admin instance, if available returns that else creates as new instance
	GetOrCreateAdmin(ctx context.Context, op messagebroker.AdminClientOptions) (messagebroker.Admin, error)

	// GetActiveConsumers returns all existing consumers. Will filter on topic name if provided else return all
	GetActiveConsumers(ctx context.Context, op messagebroker.ConsumerClientOptions) []messagebroker.Consumer

	// GetActiveProducers returns all existing producers. Will filter on topic name if provided else return all
	GetActiveProducers(ctx context.Context, op messagebroker.ProducerClientOptions) []messagebroker.Producer
}

// NewBrokerStore returns a concrete implementation IBrokerStore
func NewBrokerStore(variant string, config *messagebroker.BrokerConfig) (IBrokerStore, error) {

	if len(strings.Trim(variant, " ")) == 0 {
		return nil, fmt.Errorf("brokerstore: variant must be non-empty")
	}

	if config == nil {
		return nil, fmt.Errorf("brokerstore: broker config must be non-nil")
	}

	return &BrokerStore{
		variant: variant,
		bConfig: config,
	}, nil
}

// GetOrCreateConsumer returns for an existing consumer instance, if available returns that else creates as new instance
func (b *BrokerStore) GetOrCreateConsumer(ctx context.Context, op messagebroker.ConsumerClientOptions) (messagebroker.Consumer, error) {
	key := NewKey(b.variant, op.Partition)

	if consumer, exists := b.consumerMap.Load(key.String()); exists {
		return consumer.(messagebroker.Consumer), nil
	}

	newConsumer, perr := messagebroker.NewConsumerClient(ctx,
		b.variant,
		b.bConfig,
		&messagebroker.ConsumerClientOptions{Topic: op.Topic, Subscription: op.Subscription, GroupID: op.GroupID},
	)

	if perr != nil {
		return nil, perr
	}

	b.consumerMap.Store(key, newConsumer)

	return newConsumer, nil
}

// GetOrCreateProducer returns for an existing producer instance, if available returns that else creates as new instance
func (b *BrokerStore) GetOrCreateProducer(ctx context.Context, op messagebroker.ProducerClientOptions) (messagebroker.Producer, error) {
	key := NewKey(b.variant, op.Partition)

	if producer, exists := b.producerMap.Load(key.String()); exists {
		return producer.(messagebroker.Producer), nil
	}

	newProducer, perr := messagebroker.NewProducerClient(ctx,
		b.variant,
		b.bConfig,
		&messagebroker.ProducerClientOptions{Topic: op.Topic, TimeoutSec: op.TimeoutSec},
	)

	if perr != nil {
		return nil, perr
	}

	b.producerMap.Store(key, newProducer)

	return newProducer, nil
}

// GetOrCreateAdmin returns for an existing admin instance, if available returns that else creates as new instance
func (b *BrokerStore) GetOrCreateAdmin(ctx context.Context, options messagebroker.AdminClientOptions) (messagebroker.Admin, error) {

	if b.admin != nil {
		return b.admin, nil
	}

	admin, err := messagebroker.NewAdminClient(ctx,
		b.variant,
		b.bConfig,
		&options,
	)

	b.admin = admin

	return admin, err
}

// GetActiveConsumers returns all existing consumers. Will filter on topic name if provided else return all
func (b *BrokerStore) GetActiveConsumers(ctx context.Context, op messagebroker.ConsumerClientOptions) []messagebroker.Consumer {

	var prefix string
	if op.Topic != "" {
		prefix = NewKey(op.Topic, op.Partition).Prefix()
	}

	consumers := make([]messagebroker.Consumer, 0)
	values := findAllMatchingKeyPrefix(&b.consumerMap, prefix)
	for _, value := range values {
		consumers = append(consumers, value.(messagebroker.Consumer))
	}

	return consumers
}

// GetActiveProducers returns for an existing producer instance, if available returns that else creates as new instance
func (b *BrokerStore) GetActiveProducers(ctx context.Context, op messagebroker.ProducerClientOptions) []messagebroker.Producer {

	var prefix string
	if op.Topic != "" {
		prefix = NewKey(op.Topic, op.Partition).Prefix()
	}

	producers := make([]messagebroker.Producer, 0)
	values := findAllMatchingKeyPrefix(&b.producerMap, prefix)
	for _, value := range values {
		producers = append(producers, value.(messagebroker.Producer))
	}

	return producers
}

// iterates over the sync.Map and looks for all keys matching the given prefix
func findAllMatchingKeyPrefix(mp *sync.Map, prefix string) []interface{} {
	var values []interface{}

	mp.Range(func(key, value interface{}) bool {
		if prefix == "" {
			values = append(values, value)
		} else if strings.HasPrefix(fmt.Sprintf("%v", key), prefix) {
			values = append(values, value)
		}
		return true
	})
	return values
}
