package hooks

import (
	"context"
	"net/http"

	"github.com/rs/xid"
	"github.com/twitchtv/twirp"

	"github.com/razorpay/metro/internal/constants/contextkeys"
)

const (
	requestIDHttpHeaderKey = "X-Request-ID"
)

// RequestID returns function which puts unique request id into context.
func RequestID() *twirp.ServerHooks {
	hooks := &twirp.ServerHooks{}

	hooks.RequestRouted = func(ctx context.Context) (context.Context, error) {
		//var err error
		requestID, _ := ctx.Value(contextkeys.RequestID).(string)
		if requestID == "" {
			requestID = xid.New().String()
		}
		ctx = context.WithValue(ctx, contextkeys.RequestID, requestID)
		return ctx, nil
	}

	return hooks
}

// WithRequestID is a http handler which puts specific http request header into
// request context which is made available in twirp hooks.
// Refer: https://twitchtv.github.io/twirp/docs/headers.html
func WithRequestID(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		requestID := r.Header.Get(requestIDHttpHeaderKey)
		ctx = context.WithValue(ctx, contextkeys.RequestID, requestID)
		r = r.WithContext(ctx)
		h.ServeHTTP(w, r)
	})
}
