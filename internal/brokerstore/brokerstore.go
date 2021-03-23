package brokerstore

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/razorpay/metro/pkg/messagebroker"
	"github.com/razorpay/metro/pkg/partitionlocker"
)

// Key ...
type Key struct {
	name string // subscription name
	id   string // unique id
}

// NewKey creates a new key for broker map
func NewKey(name string, id string) *Key {
	return &Key{
		name: name,
		id:   id,
	}
}

// Prefix returns only the topic name to run a match all query
func (key *Key) Prefix() string {
	return key.name
}

func (key *Key) String() string {
	return fmt.Sprintf("%v-%v", key.name, key.id)
}

// BrokerStore holds broker clients
type BrokerStore struct {
	// stores active producer clients for a key.
	producerMap sync.Map

	// stores active consumer clients for a key
	consumerMap sync.Map

	// lock to instantiate a consumer for a key
	partitionLock *partitionlocker.PartitionLocker

	// stores an active admin client
	admin messagebroker.Admin

	// the broker variant
	variant string

	// the broker config
	bConfig *messagebroker.BrokerConfig
}

// IBrokerStore ...
type IBrokerStore interface {

	// GetConsumer returns for an existing consumer instance, if available returns that else creates as new instance
	GetConsumer(ctx context.Context, id string, op messagebroker.ConsumerClientOptions) (messagebroker.Consumer, error)

	// RemoveConsumer deletes the consumer from the store
	RemoveConsumer(ctx context.Context, id string, op messagebroker.ConsumerClientOptions) bool

	// GetProducer returns for an existing producer instance, if available returns that else creates as new instance
	GetProducer(ctx context.Context, op messagebroker.ProducerClientOptions) (messagebroker.Producer, error)

	// GetAdmin returns for an existing admin instance, if available returns that else creates as new instance
	GetAdmin(ctx context.Context, op messagebroker.AdminClientOptions) (messagebroker.Admin, error)
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
		variant:       variant,
		bConfig:       config,
		partitionLock: partitionlocker.NewPartitionLocker(&sync.Mutex{}),
	}, nil
}

// GetConsumer returns for an existing consumer instance, if available returns that else creates as new instance
func (b *BrokerStore) GetConsumer(ctx context.Context, id string, op messagebroker.ConsumerClientOptions) (messagebroker.Consumer, error) {
	key := NewKey(op.GroupID, id)
	consumer, ok := b.consumerMap.Load(key.String())
	if ok {
		return consumer.(messagebroker.Consumer), nil
	}
	b.partitionLock.Lock(key.String())              // lock
	consumer, ok = b.consumerMap.Load(key.String()) // double-check
	if ok {
		return consumer.(messagebroker.Consumer), nil
	}
	newConsumer, perr := messagebroker.NewConsumerClient(ctx,
		b.variant,
		id,
		b.bConfig,
		&op,
	)
	if perr != nil {
		return nil, perr
	}
	consumer, _ = b.consumerMap.LoadOrStore(key.String(), newConsumer)
	b.partitionLock.Unlock(key.String()) // unlock
	return consumer.(messagebroker.Consumer), nil
}

// RemoveConsumer deletes the consumer from the store
func (b *BrokerStore) RemoveConsumer(_ context.Context, id string, op messagebroker.ConsumerClientOptions) bool {
	key := NewKey(op.GroupID, id)
	_, loaded := b.consumerMap.LoadAndDelete(key)
	return loaded
}

// GetProducer returns for an existing producer instance, if available returns that else creates as new instance
func (b *BrokerStore) GetProducer(ctx context.Context, op messagebroker.ProducerClientOptions) (messagebroker.Producer, error) {
	// TODO: perf and check if single producer for a topic works
	key := NewKey(b.variant, op.Topic)
	producer, ok := b.producerMap.Load(key.String())
	if ok {
		return producer.(messagebroker.Producer), nil
	}
	b.partitionLock.Lock(key.String())              // lock
	producer, ok = b.producerMap.Load(key.String()) // double-check
	if ok {
		return producer.(messagebroker.Producer), nil
	}
	newProducer, perr := messagebroker.NewProducerClient(ctx,
		b.variant,
		b.bConfig,
		&op,
	)
	if perr != nil {
		return nil, perr
	}
	producer, _ = b.producerMap.LoadOrStore(key.String(), newProducer)
	b.partitionLock.Unlock(key.String()) // unlock
	return producer.(messagebroker.Producer), nil
}

// GetAdmin returns for an existing admin instance, if available returns that else creates as new instance
func (b *BrokerStore) GetAdmin(ctx context.Context, options messagebroker.AdminClientOptions) (messagebroker.Admin, error) {

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
