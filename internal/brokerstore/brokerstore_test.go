package brokerstore

import (
	"sync"
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

func Test_findFirstMatchingKeyPrefix(t *testing.T) {
	mp := sync.Map{}

	mp.Store("prefix1-k1", "v1")
	mp.Store("prefix2-k2", "v2")
	mp.Store("prefix4-k4", "v4")

	val1 := findFirstMatchingKeyPrefix(&mp, "prefix4")
	assert.NotNil(t, val1)

	val2 := findFirstMatchingKeyPrefix(&mp, "prefix2")
	assert.NotNil(t, val2)

	val3 := findFirstMatchingKeyPrefix(&mp, "prefix-wrong")
	assert.Nil(t, val3)
}

func Test_findAllMatchingKeyPrefix(t *testing.T) {
	mp := sync.Map{}

	mp.Store("prefix1-k1", "v1")
	mp.Store("prefix1-k2", "v2")
	mp.Store("prefix2-k4", "v3")

	val1 := findAllMatchingKeyPrefix(&mp, "prefix1")
	assert.NotNil(t, val1)
	assert.Equal(t, len(val1), 2)

	val3 := findFirstMatchingKeyPrefix(&mp, "prefix-wrong")
	assert.Nil(t, val3)
}
