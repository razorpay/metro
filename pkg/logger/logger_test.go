// +build unit

package logger

import (
	"context"
	"reflect"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestNewLogger(t *testing.T) {
	sugar, _ := NewLogger("", nil, zapcore.NewNopCore())
	type args struct {
		env       string
		serviceKV map[string]interface{}
		hookCore  zapcore.Core
	}
	tests := []struct {
		name    string
		args    args
		want    *zap.SugaredLogger
		wantErr bool
	}{
		{
			name: "Init test",
			args: args{
				env:      "",
				hookCore: zapcore.NewNopCore(),
			},
			wantErr: false,
			want:    sugar,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewLogger(tt.args.env, tt.args.serviceKV, tt.args.hookCore)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewLogger() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewLogger() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWithContext(t *testing.T) {
	baseLogger, _ := NewLogger("", nil, zapcore.NewNopCore())
	wantLogger := baseLogger.With(
		"context1", "item1",
		"context2", 123,
	)
	baseCtx := context.WithValue(context.Background(), CtxKeyType("context1"), "item1")
	finalCtx := context.WithValue(baseCtx, CtxKeyType("context2"), 123)
	type args struct {
		ctx       context.Context
		ctxFields []CtxKeyType
	}
	tests := []struct {
		name string
		args args
		want *zap.SugaredLogger
	}{
		{
			name: "Verify context fields",
			args: args{
				ctx: finalCtx,
				ctxFields: []CtxKeyType{
					"context1",
					"context2",
				},
			},
			want: wantLogger,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := WithContext(tt.args.ctx, tt.args.ctxFields); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("WithContext() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCtx(t *testing.T) {
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name string
		args args
		want *zap.SugaredLogger
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Ctx(tt.args.ctx); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Ctx() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_MapToSliceOfKV(t *testing.T) {
	type args struct {
		m map[string]interface{}
	}
	tests := []struct {
		name string
		args args
		want []interface{}
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MapToSliceOfKV(tt.args.m); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MapToSliceOfKV() = %v, want %v", got, tt.want)
			}
		})
	}
}
