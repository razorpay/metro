// +build unit

package registry

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
)

func TestNewConsulWatcherKey(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/kv/k1", func(res http.ResponseWriter, req *http.Request) {
		response := []api.KVPair{
			api.KVPair{
				Key:   "k1",
				Value: []byte(time.Now().String()),
			}}

		resBytes, err := json.Marshal(response)
		assert.Nil(t, err)
		res.Write(resBytes)
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	config := ConsulConfig{
		Config: api.Config{
			Address: ts.URL,
		},
	}

	c, err := NewConsulClient(&config)
	assert.NotNil(t, c)
	assert.Nil(t, err)

	ctx := context.Background()

	doneCh := make(chan struct{})

	w, err := c.Watch(ctx, &WatchConfig{
		WatchPath: "k1",
		WatchType: "key",
		Handler: func(ctx context.Context, pairs []Pair) {
			assert.Equal(t, len(pairs), 1)
			assert.Equal(t, pairs[0].Key, "k1")
			doneCh <- struct{}{}
		},
	})

	assert.NotNil(t, w)
	assert.Nil(t, err)

	go func() {
		<-doneCh
		w.StopWatch()
	}()

	err = w.StartWatch()
	assert.Nil(t, err)
}

func TestNewConsulWatcherKeyPrefix(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/kv/k", func(res http.ResponseWriter, req *http.Request) {
		response := []api.KVPair{
			api.KVPair{
				Key:   "k1",
				Value: []byte(time.Now().String()),
			}}

		resBytes, err := json.Marshal(response)
		assert.Nil(t, err)
		res.Write(resBytes)
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	config := ConsulConfig{
		Config: api.Config{
			Address: ts.URL,
		},
	}

	c, err := NewConsulClient(&config)
	assert.NotNil(t, c)
	assert.Nil(t, err)

	ctx := context.Background()

	doneCh := make(chan struct{})

	w, err := c.Watch(ctx, &WatchConfig{
		WatchPath: "k",
		WatchType: "keyprefix",
		Handler: func(ctx context.Context, pairs []Pair) {
			assert.Equal(t, len(pairs), 1)
			assert.Equal(t, pairs[0].Key, "k1")
			doneCh <- struct{}{}
		},
	})

	assert.NotNil(t, w)
	assert.Nil(t, err)

	go func() {
		<-doneCh
		w.StopWatch()
	}()

	err = w.StartWatch()
	assert.Nil(t, err)
}
