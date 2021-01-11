package brokerstore

import (
	"context"
	"fmt"

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

func (key *Key) String() string {
	if key.instanceType == admin {
		// admin clients are not associated with topic names
		return fmt.Sprintf("%v", key.instanceType)
	}
	return fmt.Sprintf("%v-%v", key.instanceType, key.topic)
}

// BrokerStore holds broker clients
type BrokerStore struct {
	// stores active producer clients for a key.
	producerMap map[*Key]messagebroker.Producer

	// stores active producer clients for a key
	consumerMap map[*Key]messagebroker.Consumer

	// stores active admin clients for a key
	adminMap map[*Key]messagebroker.Admin

	// the broker variant
	variant string

	// the broker config
	bConfig *messagebroker.BrokerConfig
}

// IBrokerStore ...
type IBrokerStore interface {
	// CreateConsumer returns a new consumer instance of the desired broker
	CreateConsumer(context.Context, messagebroker.ConsumerClientOptions) (messagebroker.Consumer, error)

	// GetExistingOrCreateConsumer returns for an existing consumer instance, if available returns that else creates as new instance
	GetExistingOrCreateConsumer(context.Context, messagebroker.ConsumerClientOptions) (messagebroker.Consumer, error)

	// CreateProducer returns a new producer instance of the desired broker
	CreateProducer(context.Context, messagebroker.ProducerClientOptions) (messagebroker.Producer, error)

	// GetExistingOrCreateProducer returns for an existing producer instance, if available returns that else creates as new instance
	GetExistingOrCreateProducer(context.Context, messagebroker.ProducerClientOptions) (messagebroker.Producer, error)

	// GetExistingOrCreateAdmin returns for an existing admin instance, if available returns that else creates as new instance
	GetExistingOrCreateAdmin(context.Context, messagebroker.AdminClientOptions) (messagebroker.Admin, error)
}

// NewBrokerStore returns a concrete implementation IBrokerStore
func NewBrokerStore(variant string, config *messagebroker.BrokerConfig) (IBrokerStore, error) {
	return &BrokerStore{
		variant: variant,
		bConfig: config,
	}, nil
}

// CreateConsumer returns a new consumer instance of the desired broker
func (b *BrokerStore) CreateConsumer(ctx context.Context, options messagebroker.ConsumerClientOptions) (messagebroker.Consumer, error) {
	consumerClient, perr := messagebroker.NewConsumerClient(ctx,
		b.variant,
		b.bConfig,
		&messagebroker.ConsumerClientOptions{Topic: options.Topic, Subscription: options.Subscription, GroupID: options.GroupID},
	)

	if perr != nil {
		return nil, perr
	}

	return consumerClient, nil
}

// GetExistingOrCreateConsumer returns for an existing consumer instance, if available returns that else creates as new instance
func (b *BrokerStore) GetExistingOrCreateConsumer(ctx context.Context, options messagebroker.ConsumerClientOptions) (messagebroker.Consumer, error) {
	key := NewKey(b.variant, options.Topic)

	if consumer, ok := b.consumerMap[key]; ok {
		return consumer, nil
	}

	return b.CreateConsumer(ctx, options)
}

// CreateProducer returns a new producer instance of the desired broker
func (b *BrokerStore) CreateProducer(ctx context.Context, options messagebroker.ProducerClientOptions) (messagebroker.Producer, error) {
	producerClient, perr := messagebroker.NewProducerClient(ctx,
		b.variant,
		b.bConfig,
		&messagebroker.ProducerClientOptions{Topic: options.Topic, Timeout: options.Timeout},
	)

	if perr != nil {
		return nil, perr
	}

	return producerClient, nil
}

// GetExistingOrCreateProducer returns for an existing producer instance, if available returns that else creates as new instance
func (b *BrokerStore) GetExistingOrCreateProducer(ctx context.Context, options messagebroker.ProducerClientOptions) (messagebroker.Producer, error) {
	key := NewKey(b.variant, options.Topic)

	if producer, ok := b.producerMap[key]; ok {
		return producer, nil
	}

	return b.CreateProducer(ctx, options)
}

// GetExistingOrCreateAdmin returns for an existing admin instance, if available returns that else creates as new instance
func (b *BrokerStore) GetExistingOrCreateAdmin(ctx context.Context, options messagebroker.AdminClientOptions) (messagebroker.Admin, error) {
	key := NewKey(b.variant, "")

	if admin, ok := b.adminMap[key]; ok {
		return admin, nil
	}

	admin, err := messagebroker.NewAdminClient(ctx,
		b.variant,
		b.bConfig,
		&messagebroker.AdminClientOptions{},
	)

	return admin, err
}
