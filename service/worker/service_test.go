//go:build unit

package worker

import (
	"context"
	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/tasks"
	"github.com/razorpay/metro/pkg/cache"
	"github.com/razorpay/metro/pkg/httpclient"
	"github.com/razorpay/metro/pkg/messagebroker"
	"github.com/razorpay/metro/pkg/registry"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"reflect"
	"testing"
)

func TestNewService(t *testing.T) {
	type args struct {
		workerConfig   *Config
		registryConfig *registry.Config
		cacheConfig    *cache.Config
	}
	tests := []struct {
		name      string
		args      args
		want      *Service
		wantErr   bool
		errorStr  string
		deepEqual bool
	}{
		{
			name: "invalid registry driver should result in an error",
			args: args{
				workerConfig: &Config{
					Broker: Broker{},
					Interfaces: struct {
						API NetworkInterfaces
					}{},
					HTTPClientConfig: httpclient.Config{},
				},
				registryConfig: &registry.Config{
					Driver:       "etcd",
					ConsulConfig: registry.ConsulConfig{},
				},
				cacheConfig: &cache.Config{
					Driver:       "",
					ConsulConfig: cache.ConsulConfig{},
					RedisConfig:  cache.RedisConfig{},
				},
			},
			want:     nil,
			wantErr:  true,
			errorStr: "Unknown Registry Driver: etcd",
		},
		{
			name: "invalid cache driver should result in an error",
			args: args{
				workerConfig: &Config{
					Broker: Broker{},
					Interfaces: struct {
						API NetworkInterfaces
					}{},
					HTTPClientConfig: httpclient.Config{},
				},
				registryConfig: &registry.Config{
					Driver:       "consul",
					ConsulConfig: registry.ConsulConfig{},
				},
				cacheConfig: &cache.Config{
					Driver:       "rabbitmq",
					ConsulConfig: cache.ConsulConfig{},
					RedisConfig:  cache.RedisConfig{},
				},
			},
			want:     nil,
			wantErr:  true,
			errorStr: "Unknown Cache Driver: rabbitmq",
		},
		{
			name: "invalid broker should result in an error",
			args: args{
				workerConfig: &Config{
					Broker: Broker{
						Variant:      "nakardi",
						BrokerConfig: messagebroker.BrokerConfig{},
					},
					Interfaces: struct {
						API NetworkInterfaces
					}{},
					HTTPClientConfig: httpclient.Config{},
				},
				registryConfig: &registry.Config{
					Driver: "consul",
					ConsulConfig: registry.ConsulConfig{
						Mock: true,
					},
				},
				cacheConfig: &cache.Config{
					Driver:       "redis",
					ConsulConfig: cache.ConsulConfig{},
					RedisConfig: cache.RedisConfig{
						Mock: true,
					},
				},
			},
			want:     nil,
			wantErr:  true,
			errorStr: "brokerstore: provided variant is not supported",
		},
		{
			name: "valid configs should not result in an error",
			args: args{
				workerConfig: &Config{
					Broker: Broker{
						Variant:      "kafka",
						BrokerConfig: messagebroker.BrokerConfig{},
					},
					Interfaces: struct {
						API NetworkInterfaces
					}{},
					HTTPClientConfig: httpclient.Config{},
				},
				registryConfig: &registry.Config{
					Driver: "consul",
					ConsulConfig: registry.ConsulConfig{
						Mock: true,
					},
				},
				cacheConfig: &cache.Config{
					Driver:       "redis",
					ConsulConfig: cache.ConsulConfig{},
					RedisConfig: cache.RedisConfig{
						Mock: true,
					},
				},
			},
			want:      &Service{},
			wantErr:   false,
			deepEqual: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewService(tt.args.workerConfig, tt.args.registryConfig, tt.args.cacheConfig)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewService() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.deepEqual && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewService() got = %v, want %v", got, tt.want)
			}
			if tt.wantErr {
				assert.Equal(t, err.Error(), tt.errorStr)
			}

		})
	}
}

func TestService_GetErrorChannel(t *testing.T) {
	type fields struct {
		id               string
		workerConfig     *Config
		leaderTask       tasks.ITask
		subscriptionTask tasks.ITask
		doneCh           chan struct{}
		registry         registry.IRegistry
		cache            cache.ICache
		brokerStore      brokerstore.IBrokerStore
	}
	tests := []struct {
		name   string
		fields fields
		want   chan error
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &Service{
				id:               tt.fields.id,
				workerConfig:     tt.fields.workerConfig,
				leaderTask:       tt.fields.leaderTask,
				subscriptionTask: tt.fields.subscriptionTask,
				doneCh:           tt.fields.doneCh,
				registry:         tt.fields.registry,
				cache:            tt.fields.cache,
				brokerStore:      tt.fields.brokerStore,
			}
			if got := svc.GetErrorChannel(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetErrorChannel() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestService_Start(t *testing.T) {
	type fields struct {
		id               string
		workerConfig     *Config
		leaderTask       tasks.ITask
		subscriptionTask tasks.ITask
		doneCh           chan struct{}
		registry         registry.IRegistry
		cache            cache.ICache
		brokerStore      brokerstore.IBrokerStore
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &Service{
				id:               tt.fields.id,
				workerConfig:     tt.fields.workerConfig,
				leaderTask:       tt.fields.leaderTask,
				subscriptionTask: tt.fields.subscriptionTask,
				doneCh:           tt.fields.doneCh,
				registry:         tt.fields.registry,
				cache:            tt.fields.cache,
				brokerStore:      tt.fields.brokerStore,
			}
			if err := svc.Start(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("Start() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_getInterceptors(t *testing.T) {
	tests := []struct {
		name string
		want []grpc.UnaryServerInterceptor
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getInterceptors(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getInterceptors() = %v, want %v", got, tt.want)
			}
		})
	}
}
