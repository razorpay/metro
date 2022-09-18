package tasks

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/razorpay/metro/internal/nodebinding"
	mocks5 "github.com/razorpay/metro/internal/nodebinding/mocks/core"
	"github.com/razorpay/metro/internal/topic"
	mocks4 "github.com/razorpay/metro/internal/topic/mocks/core"
	"github.com/razorpay/metro/pkg/registry"
	"github.com/razorpay/metro/pkg/registry/mocks"
)

func TestNewPublisherTask(t *testing.T) {
	type args struct {
		id              string
		registry        registry.IRegistry
		topicCore       topic.ICore
		nodeBindingCore nodebinding.ICore
		options         []Option
	}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registryMock := mocks.NewMockIRegistry(ctrl)
	topicCoreMock := mocks4.NewMockICore(ctrl)
	nodebindingCoreMock := mocks5.NewMockICore(ctrl)

	task := &PublisherTask{
		id:              uuid.New().String(),
		registry:        registryMock,
		topicCore:       topicCoreMock,
		nodeBindingCore: nodebindingCoreMock,
		topicWatchData:  make(chan *struct{}),
	}

	tests := []struct {
		name    string
		args    args
		want    ITask
		wantErr bool
	}{
		{
			name: "Creating new publisher task successfully",
			args: args{
				id:              uuid.New().String(),
				registry:        registryMock,
				topicCore:       topicCoreMock,
				nodeBindingCore: nodebindingCoreMock,
				options:         []Option{},
			},
			want:    task,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewPublisherTask(tt.args.id, tt.args.registry, tt.args.topicCore, tt.args.nodeBindingCore, tt.args.options...)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewPublisherTask() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

// func TestPublisherTask_Run(t *testing.T) {
// 	type fields struct {
// 		id              string
// 		registry        registry.IRegistry
// 		topicCore       topic.ICore
// 		nodeBindingCore nodebinding.ICore
// 		topicWatchData  chan *struct{}
// 	}
// 	type args struct {
// 		ctx context.Context
// 	}
// 	ctx, cancel := context.WithCancel(context.Background())
// 	ctrl := gomock.NewController(t)
// 	defer ctrl.Finish()
// 	// defer cancel()

// 	registryMock := mocks.NewMockIRegistry(ctrl)
// 	topicCoreMock := mocks4.NewMockICore(ctrl)
// 	nodebindingCoreMock := mocks5.NewMockICore(ctrl)
// 	watcherMock := mocks.NewMockIWatcher(ctrl)

// 	tests := []struct {
// 		name    string
// 		fields  fields
// 		args    args
// 		wantErr bool
// 	}{
// 		{
// 			name: "Run publisher task successfully",
// 			fields: fields{
// 				id:              uuid.New().String(),
// 				registry:        registryMock,
// 				topicCore:       topicCoreMock,
// 				nodeBindingCore: nodebindingCoreMock,
// 				topicWatchData:  make(chan *struct{}),
// 			},
// 			args: args{
// 				ctx: ctx,
// 			},
// 			wantErr: false,
// 		},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			pu := &PublisherTask{
// 				id:              tt.fields.id,
// 				registry:        tt.fields.registry,
// 				topicCore:       tt.fields.topicCore,
// 				nodeBindingCore: tt.fields.nodeBindingCore,
// 				topicWatchData:  tt.fields.topicWatchData,
// 			}
// 			topicCoreMock.EXPECT().List(gomock.AssignableToTypeOf(ctx), "topics/").Return([]*topic.Model{}, nil)
// 			registryMock.EXPECT().Watch(gomock.AssignableToTypeOf(ctx), gomock.Any()).Return(watcherMock, nil)
// 			if err := pu.Run(tt.args.ctx); (err != nil) != tt.wantErr {
// 				t.Errorf("PublisherTask.Run() error = %v, wantErr %v", err, tt.wantErr)
// 			}
// 		})
// 	}
// }

func TestPublisherTask_refreshCache(t *testing.T) {
	type fields struct {
		id              string
		registry        registry.IRegistry
		topicCore       topic.ICore
		nodeBindingCore nodebinding.ICore
		topicWatchData  chan *struct{}
	}
	type args struct {
		ctx context.Context
	}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx, cancel := context.WithCancel(context.Background())

	registryMock := mocks.NewMockIRegistry(ctrl)
	topicCoreMock := mocks4.NewMockICore(ctrl)
	nodebindingCoreMock := mocks5.NewMockICore(ctrl)

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Refresh cache successfully",
			fields: fields{
				id:              uuid.New().String(),
				registry:        registryMock,
				topicCore:       topicCoreMock,
				nodeBindingCore: nodebindingCoreMock,
				topicWatchData:  make(chan *struct{}),
			},
			args: args{
				ctx: ctx,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pu := &PublisherTask{
				id:              tt.fields.id,
				registry:        tt.fields.registry,
				topicCore:       tt.fields.topicCore,
				nodeBindingCore: tt.fields.nodeBindingCore,
				topicWatchData:  tt.fields.topicWatchData,
			}
			topicCoreMock.EXPECT().List(gomock.AssignableToTypeOf(ctx), "topics/").Return([]*topic.Model{}, nil)
			if err := pu.refreshCache(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("PublisherTask.refreshCache() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
	cancel()
}

func TestCheckIfTopicExists(t *testing.T) {
	type args struct {
		ctx   context.Context
		topic string
	}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Topic doesn't exist",
			args: args{
				ctx:   ctx,
				topic: "projects/test-project/topics/test-topic",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CheckIfTopicExists(tt.args.ctx, tt.args.topic); got != tt.want {
				t.Errorf("CheckIfTopicExists() = %v, want %v", got, tt.want)
			}
		})
	}
}
