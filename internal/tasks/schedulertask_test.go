// +build unit

package tasks

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	mocks2 "github.com/razorpay/metro/internal/brokerstore/mocks"
	"github.com/razorpay/metro/internal/node"
	mocks3 "github.com/razorpay/metro/internal/node/mocks/core"
	"github.com/razorpay/metro/internal/nodebinding"
	mocks5 "github.com/razorpay/metro/internal/nodebinding/mocks/core"
	mocks6 "github.com/razorpay/metro/internal/scheduler/mocks"
	"github.com/razorpay/metro/internal/subscription"
	mocks7 "github.com/razorpay/metro/internal/subscription/mocks/core"
	"github.com/razorpay/metro/internal/topic"
	mocks4 "github.com/razorpay/metro/internal/topic/mocks/core"
	"github.com/razorpay/metro/pkg/registry/mocks"
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
	registryMock.EXPECT().Watch(gomock.AssignableToTypeOf(ctx), gomock.Any()).Return(watcherMock, nil).Times(2)

	// mock watcher
	watcherMock.EXPECT().StartWatch().Do(func() {
		<-ctx.Done()
	}).Return(nil).Times(2)
	watcherMock.EXPECT().StopWatch().Times(2)

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
	subscriptionCoreMock.EXPECT().List(gomock.AssignableToTypeOf(ctx), "subscriptions/").Return(
		[]*subscription.Model{&sub}, nil)

	// mock nodes core
	nodeCoreMock.EXPECT().List(gomock.AssignableToTypeOf(ctx), "nodes/").Return(
		[]*node.Model{
			{
				ID: workerID,
			},
		}, nil)

	// mock nodebindings core
	nb := &nodebinding.Model{
		ID:             uuid.New().String(),
		NodeID:         workerID,
		SubscriptionID: "projects/test-project/subscriptions/test",
	}

	nodebindingCoreMock.EXPECT().List(gomock.AssignableToTypeOf(ctx), "nodebinding/").Return(
		[]*nodebinding.Model{}, nil)
	nodebindingCoreMock.EXPECT().List(gomock.AssignableToTypeOf(ctx), "nodebinding/").Return(
		[]*nodebinding.Model{}, nil)

	nodebindingCoreMock.EXPECT().CreateNodeBinding(gomock.AssignableToTypeOf(ctx), gomock.Any()).Return(nil).Times(2)

	// mock Topic Get
	topicCoreMock.EXPECT().Get(gomock.AssignableToTypeOf(ctx), "projects/test-project/topics/test").Return(
		&topic.Model{
			Name:               "projects/test-project/topics/test",
			NumPartitions:      1,
			ExtractedTopicName: "test",
			ExtractedProjectID: "test-project",
			Labels:             map[string]string{},
		}, nil).Times(2)

	// mock scheduler
	schedulerMock.EXPECT().Schedule(&sub, gomock.Any(), gomock.Any()).Return(nb, nil).Times(2)

	go func() {
		err = task.Run(ctx)
		assert.Equal(t, err, context.Canceled)
		close(doneCh)
	}()

	// test signals on channels
	task.(*SchedulerTask).subWatchData <- &struct{}{}
	task.(*SchedulerTask).nodeWatchData <- &struct{}{}

	// test nils
	task.(*SchedulerTask).subWatchData <- nil
	task.(*SchedulerTask).nodeWatchData <- nil

	cancel()
	<-doneCh
}
