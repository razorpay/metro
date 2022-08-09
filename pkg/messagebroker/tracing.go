package messagebroker

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// KafkaHeadersCarrier is map of string,string that contains tracing headers
type kafkaHeadersCarrier []kafka.Header

// ForeachKey conforms to the TextMapReader interface.
func (c *kafkaHeadersCarrier) ForeachKey(handler func(key, val string) error) error {
	for _, h := range *c {
		if err := handler(h.Key, string(h.Value)); err != nil {
			return err
		}
	}
	return nil
}

// Set implements Set() of opentracing.TextMapWriter.
func (c *kafkaHeadersCarrier) Set(key, val string) {
	h := kafka.Header{key, []byte(val)}
	*c = append(*c, h)
}
