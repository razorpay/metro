//go:build unit
// +build unit

package brokerstore

import (
	"context"
	"sync"
	"testing"

	"github.com/razorpay/metro/pkg/messagebroker"
	"github.com/stretchr/testify/assert"
)

func Test_NewKey(t *testing.T) {
	assert.Equal(t, "dummytopic-0", NewKey("dummytopic", "0").String())
	assert.Equal(t, "dummytopic-25", NewKey("dummytopic", "25").String())
}

func Test_NewBrokerStore(t *testing.T) {
	store1, err := NewBrokerStore("", &messagebroker.BrokerConfig{})
	assert.Nil(t, store1)
	assert.NotNil(t, err)

	store2, err := NewBrokerStore("kafka", nil)
	assert.Nil(t, store2)
	assert.NotNil(t, err)

	store3, err := NewBrokerStore("kafka", &messagebroker.BrokerConfig{})
	assert.Nil(t, err)
	assert.NotNil(t, store3)
}

func Test_findAllMatchingKeyPrefix(t *testing.T) {
	mp := sync.Map{}

	mp.Store("prefix1-k1", "v1")
	mp.Store("prefix1-k2", "v2")
	mp.Store("prefix2-k4", "v3")

	val1 := findAllMatchingKeyPrefix(&mp, "prefix1")
	assert.NotNil(t, val1)
	assert.Equal(t, len(val1), 2)

	val3 := findAllMatchingKeyPrefix(&mp, "prefix-wrong")
	assert.Nil(t, val3)
}

func TestBrokerStore_GetConsumer(t *testing.T) {
	ctx := context.Background()
	bs, _ := NewBrokerStore("kafka", getValidBrokerConfig())
	clientOptions := getValidConsumerClientOptions()
	consumer, err := bs.GetConsumer(ctx, clientOptions)
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	exists := bs.RemoveConsumer(ctx, clientOptions)
	assert.True(t, exists)
}

func TestBrokerStore_GetProducer(t *testing.T) {
	ctx := context.Background()
	bs, _ := NewBrokerStore("kafka", getValidBrokerConfig())
	clientOptions := getValidProducerClientOptions()
	producer, err := bs.GetProducer(ctx, clientOptions)
	assert.Nil(t, err)
	assert.NotNil(t, producer)
}

func TestBrokerStore_RemoveProducer(t *testing.T) {
	ctx := context.Background()
	bs, _ := NewBrokerStore("kafka", getValidBrokerConfig())
	clientOptions := getValidProducerClientOptions()
	exists := bs.RemoveProducer(ctx, clientOptions)
	assert.False(t, exists)
}

func TestBrokerStore_GetAdmin(t *testing.T) {
	ctx := context.Background()
	bs, _ := NewBrokerStore("kafka", getValidBrokerConfig())
	admin, err := bs.GetAdmin(ctx, messagebroker.AdminClientOptions{})
	assert.Nil(t, err)
	assert.NotNil(t, admin)
}

func TestBrokerStore_FlushAllProducers(t *testing.T) {
	ctx := context.Background()
	bs, _ := NewBrokerStore("kafka", getValidBrokerConfig())
	clientOptions := getValidProducerClientOptions()
	producer, err := bs.GetProducer(ctx, clientOptions)
	assert.Nil(t, err)
	assert.NotNil(t, producer)
	bs.FlushAllProducers(ctx)
}

func getValidBrokerConfig() *messagebroker.BrokerConfig {
	return &messagebroker.BrokerConfig{
		Brokers:             []string{"b1", "b2"},
		EnableTLS:           false,
		DebugEnabled:        false,
		OperationTimeoutMs:  100,
		ConnectionTimeoutMs: 100,
		Admin:               &messagebroker.AdminConfig{EnableTopicCleanUp: true},
	}
}

func getValidConsumerClientOptions() messagebroker.ConsumerClientOptions {
	return messagebroker.ConsumerClientOptions{
		Topics:          []string{"topic", "retry-topic"},
		Subscription:    "s1",
		GroupID:         "sub-id",
		GroupInstanceID: "sub-name",
	}
}

func getValidProducerClientOptions() messagebroker.ProducerClientOptions {
	return messagebroker.ProducerClientOptions{
		Topic:     "topic",
		TimeoutMs: 1000,
	}
}

func TestBrokerStore_IsTopicCleanUpEnabled(t *testing.T) {
	type fields struct {
		variant string
		bConfig *messagebroker.BrokerConfig
	}
	type args struct {
		ctx context.Context
	}
	ctx := context.Background()
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "Get EnableTopicCleanUp value as true",
			fields: fields{
				variant: "kafka",
				bConfig: getValidBrokerConfig(),
			},
			args: args{
				ctx: ctx,
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BrokerStore{
				variant: tt.fields.variant,
				bConfig: tt.fields.bConfig,
			}
			assert.Equalf(t, tt.want, b.IsTopicCleanUpEnabled(tt.args.ctx), "IsTopicCleanUpEnabled(%v)", tt.args.ctx)
		})
	}
}
