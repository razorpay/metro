package messagebroker

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

// KafkaHeadersCarrier is map of string,string that contains tracing headers
type KafkaHeadersCarrier []kafka.Header

// ForeachKey conforms to the TextMapReader interface.
func (c *KafkaHeadersCarrier) ForeachKey(handler func(key, val string) error) error {
	for _, h := range *c {
		if err := handler(h.Key, string(h.Value)); err != nil {
			return err
		}
	}
	return nil
}

// Set implements Set() of opentracing.TextMapWriter.
func (c *KafkaHeadersCarrier) Set(key, val string) {
	h := kafka.Header{key, []byte(val)}
	*c = append(*c, h)
}

type kafkaConsumerOption struct {
	messageContext opentracing.SpanContext
}

func (r kafkaConsumerOption) Apply(o *opentracing.StartSpanOptions) {
	if r.messageContext != nil {
		opentracing.ChildOf(r.messageContext).Apply(o)
	}
	ext.SpanKindConsumer.Apply(o)
}

// KafkaConsumerOption returns a StartSpanOption appropriate for a Kafka Consumer span
// with `messageContext` representing the metadata for the producer Span if available. otherwise it will be a root span
func KafkaConsumerOption(messageContext opentracing.SpanContext) opentracing.StartSpanOption {
	return kafkaConsumerOption{messageContext}
}
