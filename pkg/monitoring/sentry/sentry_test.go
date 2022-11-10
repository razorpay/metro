//go:build unit
// +build unit

package sentry

import (
	"reflect"
	"testing"

	"go.uber.org/zap/zapcore"
)

func Test_core_Write(t *testing.T) {
	type args struct {
		log zapcore.Entry
		fs  []zapcore.Field
	}
	tests := []struct {
		name    string
		c       *core
		args    args
		wantErr bool
	}{
		{
			name: "Verify No-op write",
			args: args{
				log: zapcore.Entry{},
				fs:  make([]zapcore.Field, 0),
			},
			c:       &core{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.Write(tt.args.log, tt.args.fs); (err != nil) != tt.wantErr {
				t.Errorf("core.Write() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMockHook(t *testing.T) {
	type args struct {
		log zapcore.Entry
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "basic",
			args: args{
				log: zapcore.Entry{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := MockHook(tt.args.log); (err != nil) != tt.wantErr {
				t.Errorf("MockHook() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestInitSentry(t *testing.T) {
	type args struct {
		conf    *Config
		environ string
	}
	tests := []struct {
		name    string
		args    args
		want    zapcore.Core
		wantErr bool
	}{
		{
			name: "Invalid sentry DSN",
			args: args{
				conf: &Config{
					AppName:    "stork-test",
					DSN:        "https://<invalid>@sentry.io/123",
					Mock:       false,
					ErrorLevel: 0,
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := InitSentry(tt.args.conf, tt.args.environ)
			if (err != nil) != tt.wantErr {
				t.Errorf("InitSentry() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("InitSentry() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_core_With(t *testing.T) {
	type args struct {
		fs []zapcore.Field
	}
	tests := []struct {
		name string
		c    *core
		args args
		want zapcore.Core
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.c.With(tt.args.fs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("core.With() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_core_Check(t *testing.T) {
	type args struct {
		ent zapcore.Entry
		ce  *zapcore.CheckedEntry
	}
	tests := []struct {
		name string
		c    *core
		args args
		want *zapcore.CheckedEntry
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.c.Check(tt.args.ent, tt.args.ce); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("core.Check() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_core_Sync(t *testing.T) {
	tests := []struct {
		name    string
		c       *core
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.Sync(); (err != nil) != tt.wantErr {
				t.Errorf("core.Sync() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_core_with(t *testing.T) {
	type args struct {
		fs []zapcore.Field
	}
	tests := []struct {
		name string
		c    *core
		args args
		want *core
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.c.with(tt.args.fs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("core.with() = %v, want %v", got, tt.want)
			}
		})
	}
}
