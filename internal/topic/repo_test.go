package topic

import (
	"context"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/pkg/registry"
	"github.com/razorpay/metro/pkg/registry/mocks"
	"github.com/stretchr/testify/assert"
)

func TestNewRepo(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockRegistry := mocks.NewMockIRegistry(ctrl)
	r := NewRepo(mockRegistry)
	assert.NotNil(t, r)
}

func TestRepo_List(t *testing.T) {
	type fields struct {
		BaseRepo common.BaseRepo
	}
	type args struct {
		ctx    context.Context
		prefix string
	}
	ctrl := gomock.NewController(t)
	mockRegistry := mocks.NewMockIRegistry(ctrl)
	mockRepo := common.BaseRepo{Registry: mockRegistry}
	ctx := context.Background()
	m1 := &Model{}
	m1.SetVersion("1")

	m2 := &Model{}
	m2.SetVersion("2")
	topics := []common.IModel{m1, m2}

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []common.IModel
		wantErr bool
	}{
		{
			name: "Get Models list with version set",
			fields: fields{
				BaseRepo: mockRepo,
			},
			args: args{
				ctx:    ctx,
				prefix: "sample",
			},
			want:    topics,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		r := &Repo{
			BaseRepo: tt.fields.BaseRepo,
		}
		data := []registry.Pair{
			{Key: "key1", Value: []byte("{}"), Version: "1"},
			{Key: "key2", Value: []byte("{}"), Version: "2"},
		}
		t.Run(tt.name, func(t *testing.T) {
			mockRegistry.EXPECT().List(gomock.Any(), tt.args.prefix).Return(data, nil)
			got, err := r.List(tt.args.ctx, tt.args.prefix)
			if (err != nil) != tt.wantErr {
				t.Errorf("Repo.List() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Repo.List() = %v, want %v", got, tt.want)
			}
		})
	}
}
