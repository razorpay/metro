package brokerstore

import (
	"testing"

	"github.com/razorpay/metro/pkg/messagebroker"
	"github.com/stretchr/testify/assert"
)

func Test_NewKey(t *testing.T) {
	assert.Equal(t, "producer-dummytopic", NewKey("producer", "dummytopic").Prefix())
	assert.Equal(t, "consumer-dummytopic", NewKey("consumer", "dummytopic").Prefix())
	assert.Equal(t, "admin", NewKey("admin", "dummytopic").Prefix())
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
