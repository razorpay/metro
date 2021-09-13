package httpclient

import (
	"net"
	"net/http"
	"time"
)

// Config contains config the init a new http client
type Config struct {
	ConnectTimeoutMS        int
	ConnKeepAliveMS         int
	ExpectContinueTimeoutMS int
	IdleConnTimeoutMS       int
	MaxAllIdleConns         int
	MaxHostIdleConns        int
	ResponseHeaderTimeoutMS int
	TLSHandshakeTimeoutMS   int
}

// NewClient return a http client
func NewClient(config *Config) *http.Client {
	if config == nil {
		return nil
	}

	tr := &http.Transport{
		ResponseHeaderTimeout: time.Duration(config.ResponseHeaderTimeoutMS) * time.Millisecond,
		DialContext: (&net.Dialer{
			KeepAlive: time.Duration(config.ConnKeepAliveMS) * time.Millisecond,
			Timeout:   time.Duration(config.ConnectTimeoutMS) * time.Millisecond,
		}).DialContext,
		MaxIdleConns:          config.MaxAllIdleConns,
		IdleConnTimeout:       time.Duration(config.IdleConnTimeoutMS) * time.Millisecond,
		TLSHandshakeTimeout:   time.Duration(config.TLSHandshakeTimeoutMS) * time.Millisecond,
		MaxIdleConnsPerHost:   config.MaxHostIdleConns,
		ExpectContinueTimeout: time.Duration(config.ExpectContinueTimeoutMS) * time.Millisecond,
	}

	return &http.Client{Transport: tr}
}
