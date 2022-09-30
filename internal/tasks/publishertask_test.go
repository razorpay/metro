package tasks

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/razorpay/metro/internal/topic"
	mocks4 "github.com/razorpay/metro/internal/topic/mocks/core"
	"github.com/razorpay/metro/pkg/registry"
	"github.com/razorpay/metro/pkg/registry/mocks"
	"github.com/stretchr/testify/assert"
)

// GetDummyTopicModel to export dummy topic model
func GetDummyTopicModel() []*topic.Model {
	return []*topic.Model{
		{
			Name:               "projects/test-project/topics/test",
			NumPartitions:      2,
			ExtractedTopicName: "test",
			ExtractedProjectID: "test-project",
			Labels:             map[string]string{},
		},
	}
}

func setupPT(t *testing.T) (
	ctx context.Context,
	ctrl *gomock.Controller,
	cancel context.CancelFunc,
	registryMock *mocks.MockIRegistry,
	topicCoreMock *mocks4.MockICore,
) {
	ctx, cancel = context.WithCancel(context.Background())
	ctrl = gomock.NewController(t)

	registryMock = mocks.NewMockIRegistry(ctrl)
	topicCoreMock = mocks4.NewMockICore(ctrl)
	return
}

func TestNewPublisherTask(t *testing.T) {
	type args struct {
		id        string
		registry  registry.IRegistry
		topicCore topic.ICore
		options   []Option
	}
	_, ctrl, cancel, registryMock, topicCoreMock := setupPT(t)
	defer ctrl.Finish()
	defer cancel()

	task := &PublisherTask{
		id:             uuid.New().String(),
		registry:       registryMock,
		topicCore:      topicCoreMock,
		topicWatchData: make(chan *struct{}),
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
				id:        uuid.New().String(),
				registry:  registryMock,
				topicCore: topicCoreMock,
				options:   []Option{},
			},
			want:    task,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewPublisherTask(tt.args.id, tt.args.registry, tt.args.topicCore, tt.args.options...)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewPublisherTask() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestPublisherTask_Run(t *testing.T) {
	ctx, ctrl, cancel, registryMock, topicCoreMock := setupPT(t)
	defer ctrl.Finish()
	defer cancel()
	watcherMock := mocks.NewMockIWatcher(ctrl)

	workerID := uuid.New().String()
	task, err := NewPublisherTask(
		workerID,
		registryMock,
		topicCoreMock)
	assert.Nil(t, err)

	doneCh := make(chan struct{})

	// mock registry
	registryMock.EXPECT().Watch(gomock.AssignableToTypeOf(ctx), gomock.Any()).Return(watcherMock, nil).AnyTimes()

	// mock watcher
	watcherMock.EXPECT().StartWatch().Do(func() {
		<-ctx.Done()
	}).Return(nil)
	watcherMock.EXPECT().StopWatch()

	dummyTopicModels := GetDummyTopicModel()
	// mock Topic Get
	topicCoreMock.EXPECT().List(gomock.AssignableToTypeOf(ctx), "topics/").Return(
		dummyTopicModels, nil).AnyTimes()

	go func() {
		err = task.Run(ctx)
		assert.Equal(t, err, context.Canceled)
		close(doneCh)
	}()

	// test signals on channels
	task.(*PublisherTask).topicWatchData <- &struct{}{}

	// test nils
	task.(*PublisherTask).topicWatchData <- nil

	cancel()
	<-doneCh
}

func Test2PublisherTask_Run(t *testing.T) {
	ctx, ctrl, cancel, registryMock, topicCoreMock := setupPT(t)
	defer ctrl.Finish()
	defer cancel()
	watcherMock := mocks.NewMockIWatcher(ctrl)

	workerID := uuid.New().String()
	task, err := NewPublisherTask(
		workerID,
		registryMock,
		topicCoreMock,
	)
	assert.Nil(t, err)

	doneCh := make(chan struct{})

	// mock registry
	registryMock.EXPECT().Watch(gomock.AssignableToTypeOf(ctx), gomock.Any()).Return(watcherMock, nil).AnyTimes()

	// mock watcher
	watcherMock.EXPECT().StartWatch().Do(func() {
		<-ctx.Done()
	}).Return(nil)
	watcherMock.EXPECT().StopWatch()

	dummyTopicModels := GetDummyTopicModel()
	// mock Topic Get
	topicCoreMock.EXPECT().List(gomock.AssignableToTypeOf(ctx), "topics/").Return(
		dummyTopicModels, nil).AnyTimes()

	go func() {
		err = task.Run(ctx)
		assert.Equal(t, err, context.Canceled)
		close(doneCh)
	}()

	// test signals on channels
	task.(*PublisherTask).topicWatchData <- &struct{}{}

	// test nils
	task.(*PublisherTask).topicWatchData <- nil

	cancel()
	<-doneCh
}

func TestPublisherTask_refreshCache(t *testing.T) {
	type fields struct {
		id             string
		registry       registry.IRegistry
		topicCore      topic.ICore
		topicWatchData chan *struct{}
	}
	type args struct {
		ctx context.Context
	}
	ctx, ctrl, cancel, registryMock, topicCoreMock := setupPT(t)
	defer ctrl.Finish()
	defer cancel()

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Refresh cache successfully",
			fields: fields{
				id:             uuid.New().String(),
				registry:       registryMock,
				topicCore:      topicCoreMock,
				topicWatchData: make(chan *struct{}),
			},
			args: args{
				ctx: ctx,
			},
			wantErr: false,
		},
		{
			name: "Error while getting Topic List",
			fields: fields{
				id:             uuid.New().String(),
				registry:       registryMock,
				topicCore:      topicCoreMock,
				topicWatchData: make(chan *struct{}),
			},
			args: args{
				ctx: ctx,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pu := &PublisherTask{
				id:             tt.fields.id,
				registry:       tt.fields.registry,
				topicCore:      tt.fields.topicCore,
				topicWatchData: tt.fields.topicWatchData,
			}
			if tt.wantErr == true {
				topicCoreMock.EXPECT().List(gomock.AssignableToTypeOf(ctx), "topics/").Return(nil, fmt.Errorf(""))
			} else {
				dTopic := GetDummyTopicModel()[0]
				cache := make(map[string]bool)
				cache[dTopic.Name] = true
				assert.Equal(t, topicCacheData, cache)
				topicCoreMock.EXPECT().List(gomock.AssignableToTypeOf(ctx), "topics/").Return([]*topic.Model{}, nil)
			}
			if err := pu.refreshCache(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("PublisherTask.refreshCache() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCheckIfTopicExists(t *testing.T) {
	type args struct {
		ctx   context.Context
		topic string
	}
	ctx, ctrl, cancel, _, _ := setupPT(t)
	defer ctrl.Finish()
	defer cancel()

	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Topic doesn't exist in cache",
			args: args{
				ctx:   ctx,
				topic: "projects/test-project/topics/test-topic",
			},
			want: false,
		},
		{
			name: "Topic exist in cache",
			args: args{
				ctx:   ctx,
				topic: "projects/test-project/topics/test-topic",
			},
			want: true,
		},
	}
	for _, tt := range tests {
		if tt.want == true {
			topicCacheData = make(map[string]bool)
			topicCacheData[tt.args.topic] = true
		}
		t.Run(tt.name, func(t *testing.T) {
			if got := CheckIfTopicExists(tt.args.ctx, tt.args.topic); got != tt.want {
				t.Errorf("CheckIfTopicExists() = %v, want %v", got, tt.want)
			}
		})
	}
}
