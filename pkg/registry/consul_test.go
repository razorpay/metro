// +build unit

package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
)

func TestCreateConsulClient(t *testing.T) {
	config := ConsulConfig{}
	c1, err := NewConsulClient(&config)
	assert.NotNil(t, c1)
	assert.Nil(t, err)
}

func TestRegisterSuccess(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/session/create", func(res http.ResponseWriter, req *http.Request) {
		response := struct {
			ID string
		}{ID: uuid.New().String()}

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

	sessionID, err := c.Register(context.Background(), "test", time.Second*5)
	assert.NotNil(t, sessionID)
	assert.Nil(t, err)
}

func TestRegisterFailure(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/session/create", func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(400)
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

	sessionID, err := c.Register(context.Background(), "test", time.Second*5)
	assert.Equal(t, "", sessionID)
	assert.NotNil(t, err)
}

func TestIsRegisteredSuccess(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/session/info/test-id", func(res http.ResponseWriter, req *http.Request) {
		response := []struct {
			ID string
		}{
			{ID: "test-id"},
		}

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

	registered := c.IsRegistered(context.Background(), "test-id")
	assert.Equal(t, true, registered)
}

func TestIsRegisteredFailure(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/session/info/test-id", func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(404)
		fmt.Fprint(res, []api.SessionEntry{})
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

	registered := c.IsRegistered(context.Background(), "test-id")
	assert.Equal(t, false, registered)
}

func TestIsRegisteredNotFound(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/session/info/test-id", func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		fmt.Fprint(res, []api.SessionEntry{})
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

	registered := c.IsRegistered(context.Background(), "test-id")
	assert.Equal(t, false, registered)
}

func TestRenewSuccess(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/session/renew/test-id", func(res http.ResponseWriter, req *http.Request) {
		response := []struct {
			ID string
		}{{ID: "test-id"}}

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

	err = c.Renew(context.Background(), "test-id")
	assert.Nil(t, err)
}

func TestRenewFailureSessionExpired(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/session/renew/test-id", func(res http.ResponseWriter, req *http.Request) {
		response := []api.SessionEntry{}

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

	err = c.Renew(context.Background(), "test-id")
	assert.Equal(t, api.ErrSessionExpired, err)
}

func TestRenewFailureInvalidStatus(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/session/renew/test-id", func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(400)
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

	err = c.Renew(context.Background(), "test-id")
	assert.NotNil(t, err)
}

func TestRenewPeriodicSuccess(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/session/renew/test-id", func(res http.ResponseWriter, req *http.Request) {
		response := []struct {
			ID string
		}{{ID: "test-id"}}

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

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-time.After(time.Millisecond * 15):
			cancel()
		}
	}()

	err = c.RenewPeriodic(context.Background(), "test-id", time.Millisecond*5, ctx.Done())
	assert.Nil(t, err)
}

func TestDeregisterSuccess(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/session/destroy/test-id", func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
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
	err = c.Deregister(ctx, "test-id")
	assert.Nil(t, err)
}

func TestDeregisterFailure(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/session/destroy/test-id", func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(400)
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
	err = c.Deregister(ctx, "test-id")
	assert.NotNil(t, err)
}

func TestAcquireSuccess(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/kv/k1", func(res http.ResponseWriter, req *http.Request) {
		fmt.Fprint(res, "true")
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
	acquired, err := c.Acquire(ctx, "id", "k1", []byte("v1"))
	assert.Equal(t, true, acquired)
	assert.Nil(t, err)
}

func TestReleaseSuccess(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/kv/k1", func(res http.ResponseWriter, req *http.Request) {
		fmt.Fprint(res, "true")
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
	released := c.Release(ctx, "id", "k1", "v1")
	assert.Equal(t, true, released)
}

func TestPutSuccess(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/txn", func(res http.ResponseWriter, req *http.Request) {
		resBody := `{"Results":[{"KV":{"LockIndex":0,"Key":"k1","Flags":0,"Value":"","CreateIndex":1,"ModifyIndex":1}}],"Errors":[]}`
		fmt.Fprint(res, resBody)
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
	vid, err := c.Put(ctx, "k1", []byte(""))
	assert.Nil(t, err)
	assert.Equal(t, "1", vid, "modify index not set")
}

func TestGetSuccess(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/kv/k1", func(res http.ResponseWriter, req *http.Request) {
		response := api.KVPairs{
			{
				Key:   "k1",
				Value: []byte("v1"),
			},
		}

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
	val, err := c.Get(ctx, "k1")
	t.Log(val)
	assert.Equal(t, &Pair{Key: "k1", Value: []byte("v1"), VersionID: "0", SessionID: ""}, val)
	assert.Nil(t, err)
}

func TestListSuccess(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/kv/k", func(res http.ResponseWriter, req *http.Request) {
		response := api.KVPairs{
			{
				Key:   "k1",
				Value: []byte("v1"),
			},
		}

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
	val, err := c.List(ctx, "k")
	t.Log(val)
	assert.NotNil(t, val)
}

func TestListKeysSuccess(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/kv/k", func(res http.ResponseWriter, req *http.Request) {
		response := api.KVPairs{
			{
				Key:   "k1",
				Value: []byte("v1"),
			},
		}

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
	val, err := c.ListKeys(ctx, "k")
	t.Log(val)
	assert.NotNil(t, val)
	assert.Nil(t, err)
}

func TestExistsSuccess(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/kv/k1", func(res http.ResponseWriter, req *http.Request) {
		response := api.KVPairs{
			{
				Key:   "k1",
				Value: []byte("v1"),
			},
		}

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
	val, err := c.Exists(ctx, "k1")
	t.Log(val)
	assert.Equal(t, true, val)
	assert.Nil(t, err)
}

func TestDeleteTreeSuccess(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/kv/k", func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
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
	err = c.DeleteTree(ctx, "k")
	assert.Nil(t, err)
}

func TestIsAliveSuccess(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/health/checks/any", func(res http.ResponseWriter, req *http.Request) {
		response := []api.HealthCheck{
			api.HealthCheck{Status: api.HealthPassing},
		}

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
	val, err := c.IsAlive(ctx)
	t.Log(val)
	assert.Equal(t, true, val)
	assert.Nil(t, err)
}

func TestWatchKeySuccess(t *testing.T) {
	config := ConsulConfig{}
	c, err := NewConsulClient(&config)
	assert.NotNil(t, c)
	assert.Nil(t, err)

	ctx := context.Background()
	w, err := c.Watch(ctx, &WatchConfig{
		WatchPath: "k",
		WatchType: "key",
		Handler: func(ctx context.Context, pairs []Pair) {

		},
	})

	assert.NotNil(t, w)
	assert.Nil(t, err)
}

func TestWatchKeyPrefixSuccess(t *testing.T) {
	config := ConsulConfig{}
	c, err := NewConsulClient(&config)
	assert.NotNil(t, c)
	assert.Nil(t, err)

	ctx := context.Background()
	w, err := c.Watch(ctx, &WatchConfig{
		WatchPath: "k",
		WatchType: "keyprefix",
		Handler: func(ctx context.Context, pairs []Pair) {

		},
	})

	assert.NotNil(t, w)
	assert.Nil(t, err)
}
