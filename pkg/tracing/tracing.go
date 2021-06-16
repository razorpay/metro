// Package tracing provides func to initialize opentracing tracer using jaeger client
package tracing

import (
	"fmt"
	"io"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	jaegerconfig "github.com/uber/jaeger-client-go/config"
	jaegerzap "github.com/uber/jaeger-client-go/log/zap"
	"github.com/uber/jaeger-lib/metrics/prometheus"
	"go.uber.org/zap"
)

var (
	// Tracer is used for creating spans for distributed tracing
	tracer opentracing.Tracer
	// Closer holds an instance to the RequestTracing object's Closer.
	closer io.Closer
)

// Config ... struct expected by Init func to initialize jaeger tracing client.
type Config struct {
	LogSpans    bool   // when set to true, reporter logs all submitted spans
	Host        string // jaeger-agent UDP binary thrift protocol endpoint
	Port        string // jaeger-agent UDP binary thrift protocol server port
	ServiceName string // name of this service used by tracer.
	Disabled    bool   // to mock tracer
}

// Init initialises opentracing tracer. Returns tracer for tracing spans &
// closer for flushing in-memory spans before app shutdown.
func Init(cnf Config, zlog *zap.Logger) error {
	hostPortPath := fmt.Sprintf("%s:%s", cnf.Host, cnf.Port)

	config := &jaegerconfig.Configuration{
		ServiceName: cnf.ServiceName,
		Sampler: &jaegerconfig.SamplerConfig{
			Type:  jaeger.SamplerTypeConst,
			Param: 1,
		},
		Reporter: &jaegerconfig.ReporterConfig{
			LogSpans:           cnf.LogSpans,
			LocalAgentHostPort: hostPortPath,
		},
		Disabled: cnf.Disabled,
	}

	var err error
	tracer, closer, err = config.NewTracer(
		jaegerconfig.Logger(jaegerzap.NewLogger(zlog)),
		jaegerconfig.Metrics(prometheus.New()),
	)
	if err != nil {
		return err
	}

	opentracing.SetGlobalTracer(tracer)

	return nil
}

// Close calls the closer function if initialized
func Close() error {
	if closer == nil {
		return nil
	}

	return closer.Close()
}
