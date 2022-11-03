//go:build unit
// +build unit

package tasks

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/razorpay/metro/internal/brokerstore"
	mocks2 "github.com/razorpay/metro/internal/brokerstore/mocks"
	"github.com/razorpay/metro/internal/node"
	mocks3 "github.com/razorpay/metro/internal/node/mocks/core"
	"github.com/razorpay/metro/internal/nodebinding"
	mocks5 "github.com/razorpay/metro/internal/nodebinding/mocks/core"
	"github.com/razorpay/metro/internal/scheduler"
	mocks6 "github.com/razorpay/metro/internal/scheduler/mocks"
	"github.com/razorpay/metro/internal/subscription"
	mocks7 "github.com/razorpay/metro/internal/subscription/mocks/core"
	"github.com/razorpay/metro/internal/topic"
	mocks4 "github.com/razorpay/metro/internal/topic/mocks/core"
	"github.com/razorpay/metro/pkg/registry"
	"github.com/razorpay/metro/pkg/registry/mocks"
	"github.com/stretchr/testify/assert"
)

func TestSchedulerTask_Run(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registryMock := mocks.NewMockIRegistry(ctrl)
	watcherMock := mocks.NewMockIWatcher(ctrl)
	brokerstoreMock := mocks2.NewMockIBrokerStore(ctrl)
	nodeCoreMock := mocks3.NewMockICore(ctrl)
	topicCoreMock := mocks4.NewMockICore(ctrl)
	subscriptionCoreMock := mocks7.NewMockICore(ctrl)
	nodebindingCoreMock := mocks5.NewMockICore(ctrl)
	schedulerMock := mocks6.NewMockIScheduler(ctrl)

	workerID := uuid.New().String()
	task, err := NewSchedulerTask(
		workerID,
		registryMock,
		brokerstoreMock,
		nodeCoreMock,
		topicCoreMock,
		nodebindingCoreMock,
		subscriptionCoreMock,
		schedulerMock)
	assert.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	doneCh := make(chan struct{})

	// mock registry
	registryMock.EXPECT().Watch(gomock.AssignableToTypeOf(ctx), gomock.Any()).Return(watcherMock, nil).AnyTimes()

	// mock watcher
	watcherMock.EXPECT().StartWatch().Do(func() {
		<-ctx.Done()
	}).Return(nil).Times(3)
	watcherMock.EXPECT().StopWatch().Times(3)

	// mock subscription Core
	sub := subscription.Model{
		Name:                           "projects/test-project/subscriptions/test",
		Topic:                          "projects/test-project/topics/test",
		ExtractedTopicProjectID:        "test-project",
		ExtractedSubscriptionName:      "test",
		ExtractedSubscriptionProjectID: "test-project",
		ExtractedTopicName:             "test",
		DeadLetterPolicy: &subscription.DeadLetterPolicy{
			DeadLetterTopic:     "projects/test-project/topics/test-dlq",
			MaxDeliveryAttempts: 5,
		},
		PushConfig: &subscription.PushConfig{
			PushEndpoint: "http://test.test",
		},
	}
	sub.SetVersion("1")
	subscriptionCoreMock.EXPECT().List(gomock.AssignableToTypeOf(ctx), "subscriptions/").Return(
		[]*subscription.Model{&sub}, nil).AnyTimes()

	// mock nodes core
	nodeCoreMock.EXPECT().List(gomock.AssignableToTypeOf(ctx), "nodes/").Return(
		[]*node.Model{
			{
				ID: workerID,
			},
		}, nil).AnyTimes()

	// mock nodebindings core
	nb := &nodebinding.Model{
		ID:                  uuid.New().String(),
		NodeID:              workerID,
		SubscriptionID:      "projects/test-project/subscriptions/test",
		SubscriptionVersion: "1",
	}

	nodebindingCoreMock.EXPECT().List(gomock.AssignableToTypeOf(ctx), "nodebinding/").Return(
		[]*nodebinding.Model{}, nil).AnyTimes()

	nodebindingCoreMock.EXPECT().ListKeys(gomock.AssignableToTypeOf(ctx), "nodebinding/").Return(
		[]string{}, nil).AnyTimes()

	nodebindingCoreMock.EXPECT().CreateNodeBinding(gomock.AssignableToTypeOf(ctx), gomock.Any()).Return(nil).AnyTimes()

	dummyTopicModels := []*topic.Model{
		{
			Name:               "projects/test-project/topics/test",
			NumPartitions:      2,
			ExtractedTopicName: "test",
			ExtractedProjectID: "test-project",
			Labels:             map[string]string{},
		},
	}
	// mock Topic Get
	topicCoreMock.EXPECT().List(gomock.AssignableToTypeOf(ctx), "topics/").Return(
		dummyTopicModels, nil).AnyTimes()

	// mock scheduler
	schedulerMock.EXPECT().Schedule(&sub, gomock.Any(), gomock.Any(), gomock.Any()).Return(nb, nil).AnyTimes()

	go func() {
		err = task.Run(ctx)
		assert.Equal(t, err, context.Canceled)
		close(doneCh)
	}()

	// test signals on channels
	task.(*SchedulerTask).subWatchData <- &struct{}{}
	task.(*SchedulerTask).nodeWatchData <- &struct{}{}
	task.(*SchedulerTask).topicWatchData <- &struct{}{}

	// test nils
	task.(*SchedulerTask).subWatchData <- nil
	task.(*SchedulerTask).nodeWatchData <- nil
	task.(*SchedulerTask).topicWatchData <- nil

	cancel()
	<-doneCh
}

func TestSchedulerTask_refreshCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registryMock := mocks.NewMockIRegistry(ctrl)
	brokerstoreMock := mocks2.NewMockIBrokerStore(ctrl)
	nodeCoreMock := mocks3.NewMockICore(ctrl)
	topicCoreMock := mocks4.NewMockICore(ctrl)
	subscriptionCoreMock := mocks7.NewMockICore(ctrl)
	nodebindingCoreMock := mocks5.NewMockICore(ctrl)
	schedulerMock := mocks6.NewMockIScheduler(ctrl)

	workerID := uuid.New().String()
	type fields struct {
		id               string
		registry         registry.IRegistry
		brokerstore      brokerstore.IBrokerStore
		nodeCore         node.ICore
		scheduler        scheduler.IScheduler
		topicCore        topic.ICore
		nodeBindingCore  nodebinding.ICore
		subscriptionCore subscription.ICore
		nodeCache        map[string]*node.Model
		subCache         map[string]*subscription.Model
		topicCache       map[string]*topic.Model
		nodeWatchData    chan *struct{}
		subWatchData     chan *struct{}
		topicWatchData   chan *struct{}
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
		{
			name: "Refresh cache successfully",
			fields: fields{
				id:               workerID,
				registry:         registryMock,
				brokerstore:      brokerstoreMock,
				nodeCore:         nodeCoreMock,
				scheduler:        schedulerMock,
				topicCore:        topicCoreMock,
				nodeBindingCore:  nodebindingCoreMock,
				subscriptionCore: subscriptionCoreMock,
				nodeCache:        make(map[string]*node.Model),
				subCache:         make(map[string]*subscription.Model),
				topicCache:       make(map[string]*topic.Model),
				nodeWatchData:    make(chan *struct{}),
				subWatchData:     make(chan *struct{}),
				topicWatchData:   make(chan *struct{}),
			},
			args: args{
				ctx: ctx,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := &SchedulerTask{
				id:               tt.fields.id,
				registry:         tt.fields.registry,
				brokerstore:      tt.fields.brokerstore,
				nodeCore:         tt.fields.nodeCore,
				scheduler:        tt.fields.scheduler,
				topicCore:        tt.fields.topicCore,
				nodeBindingCore:  tt.fields.nodeBindingCore,
				subscriptionCore: tt.fields.subscriptionCore,
				nodeCache:        tt.fields.nodeCache,
				subCache:         tt.fields.subCache,
				topicCache:       tt.fields.topicCache,
				nodeWatchData:    tt.fields.nodeWatchData,
				subWatchData:     tt.fields.subWatchData,
				topicWatchData:   tt.fields.topicWatchData,
			}
			dummyTopicModels := GetDummyTopicModel()
			// mock Topic Get
			topicCoreMock.EXPECT().List(gomock.AssignableToTypeOf(ctx), "topics/").Return(
				dummyTopicModels, nil).AnyTimes()
			// mock nodes core
			nodeCoreMock.EXPECT().List(gomock.AssignableToTypeOf(ctx), "nodes/").Return(
				[]*node.Model{
					{
						ID: workerID,
					},
				}, nil).AnyTimes()
			// mock subscription Core
			sub := subscription.Model{
				Name:                           "projects/test-project/subscriptions/test",
				Topic:                          "projects/test-project/topics/test",
				ExtractedTopicProjectID:        "test-project",
				ExtractedSubscriptionName:      "test",
				ExtractedSubscriptionProjectID: "test-project",
				ExtractedTopicName:             "test",
				DeadLetterPolicy: &subscription.DeadLetterPolicy{
					DeadLetterTopic:     "projects/test-project/topics/test-dlq",
					MaxDeliveryAttempts: 5,
				},
				PushConfig: &subscription.PushConfig{
					PushEndpoint: "http://test.test",
				},
			}
			sub.SetVersion("1")
			subscriptionCoreMock.EXPECT().List(gomock.AssignableToTypeOf(ctx), "subscriptions/").Return(
				[]*subscription.Model{&sub}, nil).AnyTimes()

			if err := sm.refreshCache(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("SchedulerTask.refreshCache() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func GetDummySubModel() subscription.Model {
	return subscription.Model{
		Name:                           "projects/test-project/subscriptions/test",
		Topic:                          "projects/test-project/topics/test",
		ExtractedTopicProjectID:        "test-project",
		ExtractedSubscriptionName:      "test",
		ExtractedSubscriptionProjectID: "test-project",
		ExtractedTopicName:             "test",
		DeadLetterPolicy: &subscription.DeadLetterPolicy{
			DeadLetterTopic:     "projects/test-project/topics/test-dlq",
			MaxDeliveryAttempts: 5,
		},
		PushConfig: &subscription.PushConfig{
			PushEndpoint: "http://test.test",
		},
	}
}

func TestSchedulerTask_rebalanceSubs(t *testing.T) {
	type fields struct {
		id               string
		registry         registry.IRegistry
		brokerstore      brokerstore.IBrokerStore
		nodeCore         node.ICore
		scheduler        scheduler.IScheduler
		topicCore        topic.ICore
		nodeBindingCore  nodebinding.ICore
		subscriptionCore subscription.ICore
		nodeCache        map[string]*node.Model
		subCache         map[string]*subscription.Model
		topicCache       map[string]*topic.Model
		nodeWatchData    chan *struct{}
		subWatchData     chan *struct{}
		topicWatchData   chan *struct{}
	}
	type args struct {
		ctx context.Context
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registryMock := mocks.NewMockIRegistry(ctrl)
	brokerstoreMock := mocks2.NewMockIBrokerStore(ctrl)
	nodeCoreMock := mocks3.NewMockICore(ctrl)
	topicCoreMock := mocks4.NewMockICore(ctrl)
	subscriptionCoreMock := mocks7.NewMockICore(ctrl)
	nodebindingCoreMock := mocks5.NewMockICore(ctrl)
	schedulerMock := mocks6.NewMockIScheduler(ctrl)

	workerID := uuid.New().String()
	workerID2 := uuid.New().String()
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Rebalance subs failed",
			fields: fields{
				id:               workerID,
				registry:         registryMock,
				brokerstore:      brokerstoreMock,
				nodeCore:         nodeCoreMock,
				scheduler:        schedulerMock,
				topicCore:        topicCoreMock,
				nodeBindingCore:  nodebindingCoreMock,
				subscriptionCore: subscriptionCoreMock,
				nodeCache:        make(map[string]*node.Model),
				subCache:         make(map[string]*subscription.Model),
				topicCache:       make(map[string]*topic.Model),
				nodeWatchData:    make(chan *struct{}),
				subWatchData:     make(chan *struct{}),
				topicWatchData:   make(chan *struct{}),
			},
			args: args{
				ctx: ctx,
			},
			wantErr: true,
		},
		{
			name: "Rebalance subs successfully",
			fields: fields{
				id:               workerID,
				registry:         registryMock,
				brokerstore:      brokerstoreMock,
				nodeCore:         nodeCoreMock,
				scheduler:        schedulerMock,
				topicCore:        topicCoreMock,
				nodeBindingCore:  nodebindingCoreMock,
				subscriptionCore: subscriptionCoreMock,
				nodeCache:        make(map[string]*node.Model),
				subCache:         make(map[string]*subscription.Model),
				topicCache:       make(map[string]*topic.Model),
				nodeWatchData:    make(chan *struct{}),
				subWatchData:     make(chan *struct{}),
				topicWatchData:   make(chan *struct{}),
			},
			args: args{
				ctx: ctx,
			},
			wantErr: false,
		},
	}
	// mock nodebindings core
	nb := &nodebinding.Model{
		ID:                  uuid.New().String(),
		NodeID:              workerID,
		SubscriptionID:      "projects/test-project/subscriptions/test",
		SubscriptionVersion: "1",
	}
	nb2 := &nodebinding.Model{
		ID:                  uuid.New().String(),
		NodeID:              workerID2,
		SubscriptionID:      "projects/test-project/subscriptions/test",
		SubscriptionVersion: "1",
	}
	nb3 := &nodebinding.Model{
		ID:                  uuid.New().String(),
		NodeID:              workerID2,
		SubscriptionID:      "projects/test-project/subscriptions/test",
		SubscriptionVersion: "1",
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := &SchedulerTask{
				id:               tt.fields.id,
				registry:         tt.fields.registry,
				brokerstore:      tt.fields.brokerstore,
				nodeCore:         tt.fields.nodeCore,
				scheduler:        tt.fields.scheduler,
				topicCore:        tt.fields.topicCore,
				nodeBindingCore:  tt.fields.nodeBindingCore,
				subscriptionCore: tt.fields.subscriptionCore,
				nodeCache:        tt.fields.nodeCache,
				subCache:         tt.fields.subCache,
				topicCache:       tt.fields.topicCache,
				nodeWatchData:    tt.fields.nodeWatchData,
				subWatchData:     tt.fields.subWatchData,
				topicWatchData:   tt.fields.topicWatchData,
			}

			if tt.wantErr {
				err := fmt.Errorf("Something went wrong")
				nodebindingCoreMock.EXPECT().List(gomock.AssignableToTypeOf(ctx), "nodebinding/").Return(
					nil, err)
			} else if !tt.wantErr {
				nodebindingCoreMock.EXPECT().List(gomock.AssignableToTypeOf(ctx), "nodebinding/").Return(
					[]*nodebinding.Model{
						nb, nb2, nb3,
					}, nil).AnyTimes()

				nodebindingCoreMock.EXPECT().DeleteNodeBinding(gomock.AssignableToTypeOf(ctx), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				nodebindingCoreMock.EXPECT().CreateNodeBinding(gomock.AssignableToTypeOf(ctx), gomock.Any()).Return(nil).AnyTimes()

				// mock subscription Core
				sub := GetDummySubModel()
				sub.SetVersion("1")
				sm.subCache[sub.Name] = &sub
				// mock scheduler
				schedulerMock.EXPECT().Schedule(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nb, nil).AnyTimes()
				subscriptionCoreMock.EXPECT().List(gomock.AssignableToTypeOf(ctx), "subscriptions/").Return(
					[]*subscription.Model{&sub}, nil).AnyTimes()
			}

			if err := sm.rebalanceSubscriptions(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("SchedulerTask.rebalanceSubs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
