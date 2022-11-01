package tracing

import (
	"context"
	"encoding/base64"
	"strings"

	opentracing "github.com/opentracing/opentracing-go"
	"google.golang.org/grpc/metadata"
)

// A Class is a set of types of outcomes (including errors) that will often
// be handled in the same way.
type Class string

const (
	binHdrSuffix = "-bin"
	// Success represents outcomes that achieved the desired results.
	Success Class = "2xx"
)

// metadataTextMap extends a metadata.MD to be an opentracing textmap
type metadataTextMap metadata.MD

// Set is a opentracing.TextMapReader interface that extracts values.
func (m metadataTextMap) Set(key, val string) {
	// gRPC allows for complex binary values to be written.
	encodedKey, encodedVal := encodeKeyValue(key, val)
	// The metadata object is a multimap, and previous values may exist, but for opentracing headers, we do not append
	// we just override.
	m[encodedKey] = []string{encodedVal}
}

// ForeachKey is a opentracing.TextMapReader interface that extracts values.
func (m metadataTextMap) ForeachKey(callback func(key, val string) error) error {
	for k, vv := range m {
		for _, v := range vv {
			if err := callback(k, v); err != nil {
				return err
			}
		}
	}
	return nil
}

// encodeKeyValue encodes key and value qualified for transmission via gRPC.
// note: copy pasted from private values of grpc.metadata
func encodeKeyValue(k, v string) (string, string) {
	k = strings.ToLower(k)
	if strings.HasSuffix(k, binHdrSuffix) {
		val := base64.StdEncoding.EncodeToString([]byte(v))
		v = string(val)
	}
	return k, v
}

var (
	defaultOptions = &options{
		filterOutFunc: nil,
		tracer:        nil,
	}
)

// FilterFunc allows users to provide a function that filters out certain methods from being traced.
//
// If it returns false, the given request will not be traced.
type FilterFunc func(ctx context.Context, fullMethodName string) bool

// UnaryRequestHandlerFunc is a custom request handler
type UnaryRequestHandlerFunc func(span opentracing.Span, req interface{})

// OpNameFunc is a func that allows custom operation names instead of the gRPC method.
type OpNameFunc func(method string) string

type options struct {
	filterOutFunc           FilterFunc
	tracer                  opentracing.Tracer
	traceHeaderName         string
	unaryRequestHandlerFunc UnaryRequestHandlerFunc
	opNameFunc              OpNameFunc
}

func evaluateOptions(opts []Option) *options {
	optCopy := &options{}
	*optCopy = *defaultOptions
	for _, o := range opts {
		o(optCopy)
	}
	if optCopy.tracer == nil {
		optCopy.tracer = opentracing.GlobalTracer()
	}
	if optCopy.traceHeaderName == "" {
		optCopy.traceHeaderName = "uber-trace-id"
	}

	return optCopy
}

// Option accepts a fun with options
type Option func(*options)

// WithFilterFunc customizes the function used for deciding whether a given call is traced or not.
func WithFilterFunc(f FilterFunc) Option {
	return func(o *options) {
		o.filterOutFunc = f
	}
}

// WithTraceHeaderName customizes the trace header name where trace metadata passed with requests.
// Default one is `uber-trace-id`
func WithTraceHeaderName(name string) Option {
	return func(o *options) {
		o.traceHeaderName = name
	}
}

// WithTracer sets a custom tracer to be used for this middleware, otherwise the opentracing.GlobalTracer is used.
func WithTracer(tracer opentracing.Tracer) Option {
	return func(o *options) {
		o.tracer = tracer
	}
}

// WithUnaryRequestHandlerFunc sets a custom handler for the request
func WithUnaryRequestHandlerFunc(f UnaryRequestHandlerFunc) Option {
	return func(o *options) {
		o.unaryRequestHandlerFunc = f
	}
}

// WithOpName customizes the trace Operation name
func WithOpName(f OpNameFunc) Option {
	return func(o *options) {
		o.opNameFunc = f
	}
}

// GRPCStringToGRPCCode converts message to code
var GRPCStringToGRPCCode = map[string]string{
	"OK":                  "0",
	"CANCELLED":           "1",
	"UNKNOWN":             "2",
	"INVALID_ARGUMENT":    "3",
	"DEADLINE_EXCEEDED":   "4",
	"NOT_FOUND":           "5",
	"ALREADY_EXISTS":      "6",
	"PERMISSION_DENIED":   "7",
	"RESOURCE_EXHAUSTED":  "8",
	"FAILED_PRECONDITION": "9",
	"ABORTED":             "10",
	"OUT_OF_RANGE":        "11",
	"UNIMPLEMENTED":       "12",
	"INTERNAL":            "13",
	"UNAVAILABLE":         "14",
	"DATA_LOSS":           "15",
	"UNAUTHENTICATED":     "16",
}
