// tracing package provides func to initialize opentracing tracer using jaeger client
package tracing

import (
	"io"

	"github.com/opentracing/opentracing-go"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-lib/metrics/prometheus"

	jaegerconfig "github.com/uber/jaeger-client-go/config"
)

// Config ... struct expected by Init func to initialize jaeger tracing client.
type Config struct {
	LogSpans           bool   // when set to true, reporter logs all submitted spans
	LocalAgentHostPort string // jaeger-agent UDP binary thrift protocol endpoint
	ServiceName        string // name of this service used by tracer.
	Disabled           bool   // to mock tracer
}

// Init initialises opentracing tracer. Returns tracer for tracing spans &
// closer for flushing in-memory spans before app shutdown.
func Init(cnf Config, logEntry *logger.Entry) (opentracing.Tracer, io.Closer, error) {

	config := &jaegerconfig.Configuration{
		ServiceName: cnf.ServiceName,
		Sampler: &jaegerconfig.SamplerConfig{
			Type:  jaeger.SamplerTypeConst,
			Param: 1,
		},
		Reporter: &jaegerconfig.ReporterConfig{
			LogSpans:           cnf.LogSpans,
			LocalAgentHostPort: cnf.LocalAgentHostPort,
		},
		Disabled: cnf.Disabled,
	}

	tracer, closer, err := config.NewTracer(
		jaegerconfig.Logger(&log{logEntry}),
		jaegerconfig.Metrics(prometheus.New()),
	)
	if err != nil {
		return nil, nil, err
	}

	opentracing.SetGlobalTracer(tracer)

	return tracer, closer, nil

}
