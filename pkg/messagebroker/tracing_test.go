package messagebroker

import (
	"testing"

	kafkapkg "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

func Test_kafkaHeadersCarrier(t *testing.T) {
	input := []kafkapkg.Header{
		{
			Key:   "k1",
			Value: []byte("v1"),
		},
		{
			Key:   "k2",
			Value: []byte("v2"),
		},
	}

	kafkaHeaders := kafkaHeadersCarrier(input)
	assert.Equal(t, len(kafkaHeaders), len(input))
	for index, got := range kafkaHeaders {
		assert.Equal(t, got.Key, input[index].Key)
		assert.Equal(t, got.Value, input[index].Value)
	}
}
