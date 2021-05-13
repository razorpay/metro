// +build unit

package brokerstore

import (
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
