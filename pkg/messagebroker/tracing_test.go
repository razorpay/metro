package messagebroker

import (
	"testing"

	kafkapkg "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/magiconair/properties/assert"
)

func TestKafkaHeadersCarrier(t *testing.T) {
	tests := []struct {
		key   string
		value []byte
	}{
		{
			key:   "test-header",
			value: []byte("test-value"),
		},
	}

	for _, test := range tests {
		headers := make([]kafkapkg.Header, 0, 1)
		headers = append(headers, kafkapkg.Header{
			Key:   test.key,
			Value: test.value,
		})
		carrier := KafkaHeadersCarrier(headers)
		for _, header := range carrier {
			assert.Equal(t, test.key, header.Key)
			assert.Equal(t, test.value, header.Value)
		}
	}
}
