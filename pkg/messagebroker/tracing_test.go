package messagebroker

import (
	"testing"

	kafkapkg "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

func Test_kafkaHeadersCarrier(t *testing.T) {
	headers := []kafkapkg.Header{
		{
			Key:   "k1",
			Value: []byte("v1"),
		},
		{
			Key:   "k2",
			Value: []byte("v2"),
		},
	}

	kafkaHeaders := kafkaHeadersCarrier(headers)
	for index, got := range kafkaHeaders {
		assert.Equal(t, got.Key, headers[index].Key)
		assert.Equal(t, got.Value, headers[index].Value)
	}

}
