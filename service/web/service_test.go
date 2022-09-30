package web

import (
	"context"
	"log"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/razorpay/metro/internal/app"
	"github.com/razorpay/metro/internal/credentials"
	mocks2 "github.com/razorpay/metro/internal/topic/mocks/core"
	"github.com/razorpay/metro/pkg/cache"
	configreader "github.com/razorpay/metro/pkg/config"
	"github.com/razorpay/metro/pkg/encryption"
	"github.com/razorpay/metro/pkg/monitoring/sentry"
	"github.com/razorpay/metro/pkg/registry"
	"github.com/razorpay/metro/pkg/registry/mocks"
	"github.com/razorpay/metro/pkg/tracing"
	openapiserver "github.com/razorpay/metro/service/openapi-server"
	"github.com/razorpay/metro/service/worker"
	_ "github.com/razorpay/metro/statik"
)

// AppConfig is application config
type AppConfig struct {
	App           App
	Tracing       tracing.Config
	Sentry        sentry.Config
	Web           Config
	Worker        worker.Config
	Registry      registry.Config
	Cache         cache.Config
	OpenAPIServer openapiserver.Config
	Admin         credentials.Model
	Encryption    encryption.Config
}

// App contains application-specific config values
type App struct {
	Env             string
	ServiceName     string
	ShutdownTimeout int
	ShutdownDelay   int
	GitCommitHash   string
}

func TestService_Start(t *testing.T) {
	type fields struct {
		webConfig      *Config
		registryConfig *registry.Config
		openapiConfig  *openapiserver.Config
		cacheConfig    *cache.Config
		admin          *credentials.Model
		errChan        chan error
		registryMock   *mocks.MockIRegistry
		topicCoreMock  *mocks2.MockICore
	}
	type args struct {
		ctx context.Context
	}

	ctx := context.Background()
	ctrl := gomock.NewController(t)
	registryMock := mocks.NewMockIRegistry(ctrl)
	topicCore := mocks2.NewMockICore(ctrl)
	var appConfig AppConfig
	err := configreader.NewDefaultConfig().Load(app.GetEnv(), &appConfig)
	if err != nil {
		log.Fatal(err)
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Start service successfully",
			fields: fields{
				webConfig:      &appConfig.Web,
				registryConfig: &appConfig.Registry,
				openapiConfig:  &appConfig.OpenAPIServer,
				cacheConfig:    &appConfig.Cache,
				admin:          &appConfig.Admin,
				errChan:        make(chan error),
				registryMock:   registryMock,
				topicCoreMock:  topicCore,
			},
			args: args{
				ctx: ctx,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &Service{
				webConfig:      tt.fields.webConfig,
				registryConfig: tt.fields.registryConfig,
				openapiConfig:  tt.fields.openapiConfig,
				cacheConfig:    tt.fields.cacheConfig,
				admin:          tt.fields.admin,
				errChan:        tt.fields.errChan,
			}
			if err := svc.Start(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("Service.Start() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
