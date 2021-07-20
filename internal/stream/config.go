package stream

// HTTPClientConfig contains config the init a new http client
type HTTPClientConfig struct {
	ConnectTimeoutMS        int
	ConnKeepAliveMS         int
	ExpectContinueTimeoutMS int
	IdleConnTimeoutMS       int
	MaxAllIdleConns         int
	MaxHostIdleConns        int
	ResponseHeaderTimeoutMS int
	TLSHandshakeTimeoutMS   int
}
