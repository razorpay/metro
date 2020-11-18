package cache

import (
	"reflect"
	"testing"
	"time"

	cache "github.com/patrickmn/go-cache"
)

func TestNewCacheInstance(t *testing.T) {
	mem, _ := NewCacheInstance(&InMemoryConfig{
		defaultExpiration: 15,
		cleanupInterval:   30,
	})
	type args struct {
		conf *InMemoryConfig
	}
	tests := []struct {
		name    string
		args    args
		want    *InMemoryCache
		wantErr bool
	}{
		{
			name: "Init test",
			args: args{
				conf: &InMemoryConfig{
					defaultExpiration: 15,
					cleanupInterval:   30,
				},
			},
			want:    mem,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewCacheInstance(tt.args.conf)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewCacheInstance() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewCacheInstance() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInMemoryCache_Set(t *testing.T) {
	inst, _ := NewCacheInstance(&InMemoryConfig{
		defaultExpiration: 15,
		cleanupInterval:   30,
	})
	type fields struct {
		mem *cache.Cache
	}
	type args struct {
		key   string
		value string
		ttl   time.Duration
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Init test",
			args: args{
				key:   "test-set",
				value: "test-set-val",
				ttl:   time.Minute * 1,
			},
			fields:  fields{mem: inst.mem},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			imc := &InMemoryCache{
				mem: tt.fields.mem,
			}
			if err := imc.Set(tt.args.key, tt.args.value, tt.args.ttl); (err != nil) != tt.wantErr {
				t.Errorf("InMemoryCache.Set() error = %v, wantErr %v", err, tt.wantErr)
			}
			if val, _ := imc.Get(tt.args.key); val != tt.args.value {
				t.Errorf("InMemoryCache.Set() value = %v, want %v", tt.args.value, val)
			}

		})
	}
}

func TestInMemoryCache_Get(t *testing.T) {
	inst, _ := NewCacheInstance(&InMemoryConfig{
		defaultExpiration: 15,
		cleanupInterval:   30,
	})
	type fields struct {
		mem *cache.Cache
	}
	type args struct {
		key string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
		want1  error
	}{
		{
			name: "Get test-valid",
			args: args{
				key: "test-set",
			},
			fields: fields{mem: inst.mem},
			want:   "test-set-value",
			want1:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			imc := &InMemoryCache{
				mem: tt.fields.mem,
			}
			if err := imc.Set(tt.args.key, tt.want, 1*time.Minute); err != nil {
				t.Errorf("error setting key: %v", err)
			}

			got, got1 := imc.Get(tt.args.key)
			if got != tt.want {
				t.Errorf("InMemoryCache.Get() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("InMemoryCache.Get() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestInMemoryCache_Delete(t *testing.T) {
	// mem, _ := NewCacheInstance(&InMemoryConfig{
	// 	defaultExpiration: 15,
	// 	cleanupInterval:   30,
	// })
	type fields struct {
		mem *cache.Cache
	}
	type args struct {
		key string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			imc := &InMemoryCache{
				mem: tt.fields.mem,
			}
			if err := imc.Delete(tt.args.key); err != nil {
				t.Errorf("error deleting key: %v", err)
			}
		})
	}
}
