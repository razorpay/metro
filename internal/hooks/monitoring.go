package hooks

//import (
//	"net/http"
//
//	"github.com/razorpay/metro/pkg/monitoring/newrelic"
//	"github.com/razorpay/metro/pkg/monitoring/prometheus"
//)
//
//// WithNewRelicTxn intercepts http request with newrelic transaction
//func WithNewRelicTxn(h http.Handler) http.Handler {
//	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
//		path := r.URL.EscapedPath()
//		m := newrelic.StartHTTPTxn(path, w, r)
//		defer m.End()
//		h.ServeHTTP(m, m.RequestFromTxnCtx(r))
//	})
//}
//
//// WithPrometheusTxn intercepts http request with prometheus transaction
//func WithPrometheusTxn(h http.Handler) http.Handler {
//	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
//		path := r.URL.EscapedPath()
//		m := prometheus.StartHTTPTxn(path, w, r)
//		defer m.End()
//		h.ServeHTTP(m, r)
//	})
//}
