package brokerstore

import (
	"github.com/razorpay/metro/pkg/messagebroker"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_NewKey(t *testing.T) {
	assert.Equal(t, "producer-dummytopic", NewKey("producer", "dummytopic").String())
	assert.Equal(t, "consumer-dummytopic", NewKey("consumer", "dummytopic").String())
	assert.Equal(t, "admin", NewKey("admin", "dummytopic").String())
}

func Test_NewBrokerStore1(t *testing.T) {
	store, err := NewBrokerStore("", &messagebroker.BrokerConfig{})
	assert.Nil(t, store)
	assert.NotNil(t, err)
}

func Test_NewBrokerStore2(t *testing.T) {
	store, err := NewBrokerStore("kafka", nil)
	assert.Nil(t, store)
	assert.NotNil(t, err)
}

func Test_NewBrokerStore3(t *testing.T) {
	store, err := NewBrokerStore("kafka", &messagebroker.BrokerConfig{})
	assert.Nil(t, err)
	assert.NotNil(t, store)
}
