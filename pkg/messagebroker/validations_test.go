package messagebroker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_validateKafkaConsumerClientConfig(t *testing.T) {
	op1 := ConsumerClientOptions{
		Topics:  []string{"t1"},
		GroupID: "g1",
	}

	assert.Nil(t, validateKafkaConsumerClientConfig(&op1))

	ops := []ConsumerClientOptions{
		{
			Topics:  nil,
			GroupID: "",
		}, {
			Topics:  []string{},
			GroupID: "g3",
		},
	}

	for _, op := range ops {
		assert.NotNil(t, validateKafkaConsumerClientConfig(&op))
	}
}

func Test_validateKafkaConsumerBrokerConfig(t *testing.T) {
	bc1 := BrokerConfig{
		Brokers:              []string{"kakfa-broker-1:9092"},
		OperationTimeoutSec:  5,
		ConnectionTimeoutSec: 10,
	}

	assert.Nil(t, validateKafkaConsumerBrokerConfig(&bc1))

	bcs := []BrokerConfig{
		{
			Brokers:              nil,
			OperationTimeoutSec:  3,
			ConnectionTimeoutSec: 5,
		}, {
			Brokers:              []string{"kakfa-broker-1:9092"},
			OperationTimeoutSec:  9999,
			ConnectionTimeoutSec: 10,
		},
		{
			Brokers:              []string{"kakfa-broker-1:9092"},
			OperationTimeoutSec:  2,
			ConnectionTimeoutSec: 9999,
		},
	}

	for _, bc := range bcs {
		assert.NotNil(t, validateKafkaConsumerBrokerConfig(&bc))
	}
}

func Test_validateKafkaProducerClientConfig(t *testing.T) {
	op1 := ProducerClientOptions{
		Topic:      "t1",
		Partition:  0,
		TimeoutSec: 5,
	}

	assert.Nil(t, validateKafkaProducerClientConfig(&op1))

	ops := []ProducerClientOptions{
		{
			Topic:      "t2",
			Partition:  -1,
			TimeoutSec: 5,
		},
		{
			Topic:      "",
			Partition:  0,
			TimeoutSec: 3,
		},
		{
			Topic:      "t3",
			Partition:  0,
			TimeoutSec: 300,
		},
	}

	for _, op := range ops {
		assert.NotNil(t, validateKafkaProducerClientConfig(&op))
	}

}
