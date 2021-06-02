package brokerstore

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/razorpay/metro/pkg/logger"

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
		producerMap:   sync.Map{},
		consumerMap:   sync.Map{},
		partitionLock: partitionlocker.NewPartitionLocker(&sync.Mutex{}),
	}, nil
}

// GetConsumer returns for an existing consumer instance, if available returns that else creates as new instance
func (b *BrokerStore) GetConsumer(ctx context.Context, id string, op messagebroker.ConsumerClientOptions) (messagebroker.Consumer, error) {
	brokerStoreOperationCount.WithLabelValues(env, "GetConsumer").Inc()

	startTime := time.Now()
	defer func() {
		brokerStoreOperationTimeTaken.WithLabelValues(env, "GetConsumer").Observe(time.Now().Sub(startTime).Seconds())
	}()

	key := NewKey(op.GroupID, id)
	consumer, ok := b.consumerMap.Load(key.String())
	if ok {
		return consumer.(messagebroker.Consumer), nil
	}
	b.partitionLock.Lock(key.String())         // lock
	defer b.partitionLock.Unlock(key.String()) // unlock

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

	brokerStoreActiveConsumersCount.WithLabelValues(env, key.String()).Inc()
	consumer, _ = b.consumerMap.LoadOrStore(key.String(), newConsumer)
	return consumer.(messagebroker.Consumer), nil
}

// RemoveConsumer deletes the consumer from the store
func (b *BrokerStore) RemoveConsumer(ctx context.Context, id string, op messagebroker.ConsumerClientOptions) bool {
	logger.Ctx(ctx).Infow("brokerstore: request to close consumer", "id", id, "groupID", op.GroupID)

	brokerStoreOperationCount.WithLabelValues(env, "RemoveConsumer").Inc()

	startTime := time.Now()
	defer func() {
		brokerStoreOperationTimeTaken.WithLabelValues(env, "RemoveConsumer").Observe(time.Now().Sub(startTime).Seconds())
	}()

	wasConsumerFound := false

	key := NewKey(op.GroupID, id)
	b.partitionLock.Lock(key.String())         // lock
	defer b.partitionLock.Unlock(key.String()) // unlock

	consumer, ok := b.consumerMap.Load(key.String())
	if ok {
		wasConsumerFound = true
		b.consumerMap.Delete(consumer)
		brokerStoreActiveConsumersCount.WithLabelValues(env, key.String()).Dec()
	}

	if wasConsumerFound {
		logger.Ctx(ctx).Infow("brokerstore: consumer removal completed", "id", id, "group_id", op.GroupID)
	}

	return wasConsumerFound
}

// GetProducer returns for an existing producer instance, if available returns that else creates as new instance
func (b *BrokerStore) GetProducer(ctx context.Context, op messagebroker.ProducerClientOptions) (messagebroker.Producer, error) {
	brokerStoreOperationCount.WithLabelValues(env, "GetProducer").Inc()

	startTime := time.Now()
	defer func() {
		brokerStoreOperationTimeTaken.WithLabelValues(env, "GetProducer").Observe(time.Now().Sub(startTime).Seconds())
	}()

	// TODO: perf and check if single producer for a topic works
	key := NewKey(b.variant, op.Topic)
	producer, ok := b.producerMap.Load(key.String())
	if ok {
		return producer.(messagebroker.Producer), nil
	}
	b.partitionLock.Lock(key.String())         // lock
	defer b.partitionLock.Unlock(key.String()) // unlock

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
	return producer.(messagebroker.Producer), nil
}

// GetAdmin returns for an existing admin instance, if available returns that else creates as new instance
func (b *BrokerStore) GetAdmin(ctx context.Context, options messagebroker.AdminClientOptions) (messagebroker.Admin, error) {
	brokerStoreOperationCount.WithLabelValues(env, "GetAdmin").Inc()

	startTime := time.Now()
	defer func() {
		brokerStoreOperationTimeTaken.WithLabelValues(env, "GetAdmin").Observe(time.Now().Sub(startTime).Seconds())
	}()

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
