//go:build unit
// +build unit

package messagebroker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_validateKafkaConsumerClientConfig(t *testing.T) {
	op1 := ConsumerClientOptions{
		Topics: []TopicPartition{
			{
				Topic:     "t1",
				Partition: 0,
			},
		},
		GroupID: "g1",
	}

	assert.Nil(t, validateKafkaConsumerClientConfig(&op1))

	ops := []ConsumerClientOptions{
		{
			Topics: []TopicPartition{
				{
					Topic:     "t1",
					Partition: 0,
				},
			},
			GroupID: "",
		}, {
			Topics:  []TopicPartition{},
			GroupID: "g3",
		},
	}

	for _, op := range ops {
		assert.NotNil(t, validateKafkaConsumerClientConfig(&op))
	}
}

func Test_validateKafkaConsumerBrokerConfig(t *testing.T) {
	bc1 := BrokerConfig{
		Brokers:             []string{"kakfa-broker-1:9092"},
		OperationTimeoutMs:  5,
		ConnectionTimeoutMs: 10,
	}

	assert.Nil(t, validateKafkaConsumerBrokerConfig(&bc1))

	bcs := []BrokerConfig{
		{
			Brokers:             nil,
			OperationTimeoutMs:  3,
			ConnectionTimeoutMs: 5,
		}, {
			Brokers:             []string{"kakfa-broker-1:9092"},
			OperationTimeoutMs:  99999999,
			ConnectionTimeoutMs: 10,
		},
		{
			Brokers:             []string{"kakfa-broker-1:9092"},
			OperationTimeoutMs:  2,
			ConnectionTimeoutMs: 99999999,
		},
	}

	for _, bc := range bcs {
		assert.NotNil(t, validateKafkaConsumerBrokerConfig(&bc))
	}
}

func Test_validateKafkaProducerClientConfig(t *testing.T) {
	op1 := ProducerClientOptions{
		Topic:     "t1",
		TimeoutMs: 100,
	}

	assert.Nil(t, validateKafkaProducerClientConfig(&op1))

	ops := []ProducerClientOptions{
		{
			Topic:     "",
			TimeoutMs: 100,
		},
		{
			Topic:     "t3",
			TimeoutMs: 300000,
		},
	}

	for _, op := range ops {
		assert.NotNil(t, validateKafkaProducerClientConfig(&op))
	}
}

func Test_validateKafkaProducerBrokerConfig(t *testing.T) {
	bc1 := BrokerConfig{
		Brokers: []string{"kakfa-broker-1:9092"},
	}
	assert.Nil(t, validateKafkaProducerBrokerConfig(&bc1))

	bc2 := BrokerConfig{
		Brokers: nil,
	}
	assert.NotNil(t, validateKafkaProducerBrokerConfig(&bc2))
}

func Test_validateKafkaAdminBrokerConfig(t *testing.T) {
	bc1 := BrokerConfig{
		Brokers: []string{"kakfa-broker-1:9092"},
	}
	assert.Nil(t, validateKafkaAdminBrokerConfig(&bc1))

	bc2 := BrokerConfig{
		Brokers: nil,
	}
	assert.NotNil(t, validateKafkaAdminBrokerConfig(&bc2))
}
func Test_validateKafkaAdminClientConfig(t *testing.T) {
	assert.Nil(t, validateKafkaAdminClientConfig(&AdminClientOptions{}))
}

func Test_validatePulsarClientConfigs(t *testing.T) {
	assert.Nil(t, validatePulsarProducerClientConfig(&ProducerClientOptions{}))
	assert.Nil(t, validatePulsarConsumerClientConfig(&ConsumerClientOptions{}))
	assert.Nil(t, validatePulsarAdminClientConfig(&AdminClientOptions{}))
}

func Test_validatePulsarBrokerConfig(t *testing.T) {
	bc1 := BrokerConfig{
		Brokers: []string{"pulsar:8080"},
	}

	assert.Nil(t, validatePulsarProducerBrokerConfig(&bc1))
	assert.Nil(t, validatePulsarConsumerBrokerConfig(&bc1))
	assert.Nil(t, validatePulsarAdminBrokerConfig(&bc1))

	bcs := []BrokerConfig{
		{
			Brokers: nil,
		}, {
			Brokers: []string{"pulsar-1:8080", "pulsar-2:8080"},
		},
	}

	for _, bc := range bcs {
		assert.NotNil(t, validatePulsarProducerBrokerConfig(&bc))
		assert.NotNil(t, validatePulsarConsumerBrokerConfig(&bc))
		assert.NotNil(t, validatePulsarAdminBrokerConfig(&bc))
	}
}
