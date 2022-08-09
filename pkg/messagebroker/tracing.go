package messagebroker

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

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

type spanContextOption struct {
	messageContext opentracing.SpanContext
}

func (r spanContextOption) Apply(o *opentracing.StartSpanOptions) {
	if r.messageContext != nil {
		opentracing.ChildOf(r.messageContext).Apply(o)
	}
	ext.SpanKindConsumer.Apply(o)
}

// SpanContextOption returns a StartSpanOption appropriate for a Consumer span
// with `messageContext` representing the metadata for the producer Span if available. otherwise it will be a root span
func SpanContextOption(messageContext opentracing.SpanContext) opentracing.StartSpanOption {
	return spanContextOption{messageContext}
}
