package brokerstore

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/google/uuid"

	"github.com/razorpay/metro/pkg/messagebroker"
)

// InstanceType ...
type InstanceType string

const (
	producer InstanceType = "producer"
	consumer InstanceType = "consumer"
	admin    InstanceType = "admin"
)

// Key ...
type Key struct {
	instanceType InstanceType // producer, consumer, admin
	topic        string       // topic name
}

// NewKey creates a new key for broker map
func NewKey(instanceType, topic string) *Key {
	return &Key{
		instanceType: InstanceType(instanceType),
		topic:        topic,
	}
}

// Prefix constructs the key prefix without the uuid
func (key *Key) Prefix() string {
	if key.instanceType == admin {
		// admin clients are not associated with topic names
		return fmt.Sprintf("%v", key.instanceType)
	}
	return fmt.Sprintf("%v-%v", key.instanceType, key.topic)
}

func (key *Key) String() string {
	return fmt.Sprintf("%v-%v", key.Prefix(), uuid.New())
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

	// CreateConsumer returns for an existing consumer instance, if available returns that else creates as new instance
	CreateConsumer(ctx context.Context, op messagebroker.ConsumerClientOptions, getExisting bool) (messagebroker.Consumer, error)

	// CreateProducer returns for an existing producer instance, if available returns that else creates as new instance
	CreateProducer(ctx context.Context, op messagebroker.ProducerClientOptions, getExisting bool) (messagebroker.Producer, error)

	// CreateAdmin returns for an existing admin instance, if available returns that else creates as new instance
	CreateAdmin(ctx context.Context, op messagebroker.AdminClientOptions) (messagebroker.Admin, error)

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

// CreateConsumer returns for an existing consumer instance, if available returns that else creates as new instance
func (b *BrokerStore) CreateConsumer(ctx context.Context, op messagebroker.ConsumerClientOptions, getExisting bool) (messagebroker.Consumer, error) {
	key := NewKey(b.variant, op.Topic)

	if getExisting {
		consumer := findFirstMatchingKeyPrefix(&b.consumerMap, key.Prefix())
		if consumer != nil {
			return consumer.(messagebroker.Consumer), nil
		}
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

// CreateProducer returns for an existing producer instance, if available returns that else creates as new instance
func (b *BrokerStore) CreateProducer(ctx context.Context, op messagebroker.ProducerClientOptions, getExisting bool) (messagebroker.Producer, error) {
	key := NewKey(b.variant, op.Topic)

	if getExisting {
		producer := findFirstMatchingKeyPrefix(&b.producerMap, key.Prefix())
		if producer != nil {
			return producer.(messagebroker.Producer), nil
		}
	}

	newProducer, perr := messagebroker.NewProducerClient(ctx,
		b.variant,
		b.bConfig,
		&messagebroker.ProducerClientOptions{Topic: op.Topic, Timeout: op.Timeout},
	)

	if perr != nil {
		return nil, perr
	}

	b.producerMap.Store(key, newProducer)

	return newProducer, nil
}

// CreateAdmin returns for an existing admin instance, if available returns that else creates as new instance
func (b *BrokerStore) CreateAdmin(ctx context.Context, options messagebroker.AdminClientOptions) (messagebroker.Admin, error) {

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
	if op.Topic == "" {
		prefix = string(consumer)
	} else {
		prefix = NewKey(b.variant, op.Topic).Prefix()
	}

	var consumers []messagebroker.Consumer
	values := findAllMatchingKeyPrefix(&b.consumerMap, prefix)
	for _, value := range values {
		consumers = append(consumers, value.(messagebroker.Consumer))
	}

	return consumers
}

// GetActiveProducers returns for an existing producer instance, if available returns that else creates as new instance
func (b *BrokerStore) GetActiveProducers(ctx context.Context, op messagebroker.ProducerClientOptions) []messagebroker.Producer {
	var prefix string
	if op.Topic == "" {
		prefix = string(producer)
	} else {
		prefix = NewKey(b.variant, op.Topic).Prefix()
	}

	var producers []messagebroker.Producer
	values := findAllMatchingKeyPrefix(&b.consumerMap, prefix)
	for _, value := range values {
		producers = append(producers, value.(messagebroker.Producer))
	}

	return producers
}

// iterates over the sync.Map and looks for the first key matching the given prefix
func findFirstMatchingKeyPrefix(mp *sync.Map, prefix string) interface{} {
	var val interface{}

	mp.Range(func(key, value interface{}) bool {
		if strings.HasPrefix(fmt.Sprintf("%v", key), prefix) {
			val = value
		}
		return true
	})

	return val
}

// iterates over the sync.Map and looks for all keys matching the given prefix
func findAllMatchingKeyPrefix(mp *sync.Map, prefix string) []interface{} {
	var values []interface{}

	mp.Range(func(key, value interface{}) bool {
		if strings.HasPrefix(fmt.Sprintf("%v", key), prefix) {
			values = append(values, value)
		}
		return true
	})
	return values
}
