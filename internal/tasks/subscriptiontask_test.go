//go:build unit
// +build unit

package tasks

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/razorpay/metro/pkg/httpclient"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	mocks2 "github.com/razorpay/metro/internal/brokerstore/mocks"
	"github.com/razorpay/metro/internal/nodebinding"
	mocks5 "github.com/razorpay/metro/internal/nodebinding/mocks/core"
	"github.com/razorpay/metro/internal/subscriber"
	mocks6 "github.com/razorpay/metro/internal/subscriber/mocks"
	"github.com/razorpay/metro/internal/subscription"
	mocks4 "github.com/razorpay/metro/internal/subscription/mocks/core"
	"github.com/razorpay/metro/pkg/registry"
	"github.com/razorpay/metro/pkg/registry/mocks"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

func TestSubscriptionTask_Run(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registryMock := mocks.NewMockIRegistry(ctrl)
	brokerstoreMock := mocks2.NewMockIBrokerStore(ctrl)
	watcherMock := mocks.NewMockIWatcher(ctrl)
	subscriptionCoreMock := mocks4.NewMockICore(ctrl)
	nodebindingCoreMock := mocks5.NewMockICore(ctrl)
	subscriberCoreMock := mocks6.NewMockICore(ctrl)

	workerID := uuid.New().String()
	httpConfig := &httpclient.Config{}
	task, err := NewSubscriptionTask(
		workerID,
		registryMock,
		brokerstoreMock,
		subscriptionCoreMock,
		nodebindingCoreMock,
		subscriberCoreMock,
		WithHTTPConfig(httpConfig))
	assert.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Mock registry calls
	registryMock.EXPECT().Watch(gomock.AssignableToTypeOf(ctx), gomock.Any()).Return(watcherMock, nil)

	ErrTest := errors.New("test")

	// Mock Watcher calls
	watcherMock.EXPECT().StartWatch().Return(ErrTest).AnyTimes()
	watcherMock.EXPECT().StopWatch().AnyTimes()

	err = task.Run(ctx)
	assert.Equal(t, err, ErrTest)
}

func TestSubscriptionTask_createNodeBindingWatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registryMock := mocks.NewMockIRegistry(ctrl)
	brokerstoreMock := mocks2.NewMockIBrokerStore(ctrl)
	watcherMock := mocks.NewMockIWatcher(ctrl)
	subscriptionCoreMock := mocks4.NewMockICore(ctrl)
	nodebindingCoreMock := mocks5.NewMockICore(ctrl)
	subscriberCoreMock := mocks6.NewMockICore(ctrl)

	workerID := uuid.New().String()
	httpConfig := &httpclient.Config{}
	task, err := NewSubscriptionTask(
		workerID,
		registryMock,
		brokerstoreMock,
		subscriptionCoreMock,
		nodebindingCoreMock,
		subscriberCoreMock,
		WithHTTPConfig(httpConfig))

	assert.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Mock registry calls
	registryMock.EXPECT().Watch(gomock.AssignableToTypeOf(ctx), gomock.Any()).Return(watcherMock, nil)

	err = task.(*SubscriptionTask).createNodeBindingWatch(ctx)
	assert.Nil(t, err)
	assert.NotNil(t, task.(*SubscriptionTask).watcher)
}

func TestSubscriptionTask_startNodeBindingWatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registryMock := mocks.NewMockIRegistry(ctrl)
	brokerstoreMock := mocks2.NewMockIBrokerStore(ctrl)
	watcherMock := mocks.NewMockIWatcher(ctrl)
	subscriptionCoreMock := mocks4.NewMockICore(ctrl)
	nodebindingCoreMock := mocks5.NewMockICore(ctrl)
	subscriberCoreMock := mocks6.NewMockICore(ctrl)

	workerID := uuid.New().String()
	httpConfig := &httpclient.Config{}
	task, err := NewSubscriptionTask(
		workerID,
		registryMock,
		brokerstoreMock,
		subscriptionCoreMock,
		nodebindingCoreMock,
		subscriberCoreMock,
		WithHTTPConfig(httpConfig))

	assert.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	// Mock Watcher calls
	watcherMock.EXPECT().StartWatch().Do(func() {
		cancel()
	}).Return(nil).AnyTimes()
	watcherMock.EXPECT().StopWatch().AnyTimes()

	task.(*SubscriptionTask).watcher = watcherMock
	task.(*SubscriptionTask).startNodeBindingWatch(ctx)
}

func TestSubscriptionTask_handleNodeBindingUpdates(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registryMock := mocks.NewMockIRegistry(ctrl)
	brokerstoreMock := mocks2.NewMockIBrokerStore(ctrl)
	subscriptionCoreMock := mocks4.NewMockICore(ctrl)
	nodebindingCoreMock := mocks5.NewMockICore(ctrl)
	subscriberCoreMock := mocks6.NewMockICore(ctrl)
	subscriberMock := mocks6.NewMockISubscriber(ctrl)

	workerID := uuid.New().String()
	httpConfig := &httpclient.Config{}
	task, err := NewSubscriptionTask(
		workerID,
		registryMock,
		brokerstoreMock,
		subscriptionCoreMock,
		nodebindingCoreMock,
		subscriberCoreMock,
		WithHTTPConfig(httpConfig))

	assert.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())

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

	// Mock subscriptionCore
	subscriptionCoreMock.EXPECT().Get(gomock.Any(), "projects/test-project/subscriptions/test").Return(&sub, nil)

	// Mock subscriberCore
	subscriberCoreMock.EXPECT().NewSubscriber(
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any()).Return(subscriberMock, nil).AnyTimes()

	// mock subscriber
	reqCh := make(chan *subscriber.PullRequest)
	resCh := make(chan *metrov1.PullResponse)
	subscriberMock.EXPECT().GetErrorChannel().AnyTimes()
	subscriberMock.EXPECT().GetID().AnyTimes()
	subscriberMock.EXPECT().GetRequestChannel().Return(reqCh).AnyTimes()
	subscriberMock.EXPECT().GetResponseChannel().Return(resCh).AnyTimes()
	subscriberMock.EXPECT().Stop().Do(func() {
		close(resCh)
	}).AnyTimes()

	doneCh := make(chan struct{})

	go func() {
		for {
			select {
			case <-doneCh:
				close(resCh)
			default:
				<-reqCh
				resCh <- nil
			}
		}
	}()

	go func() {
		err := task.(*SubscriptionTask).handleWatchUpdates(ctx)
		assert.Equal(t, err, context.Canceled)
		close(doneCh)
	}()

	nb := nodebinding.Model{
		ID:             uuid.New().String(),
		NodeID:         workerID,
		SubscriptionID: "projects/test-project/subscriptions/test",
	}

	nbBytes, err := json.Marshal(nb)
	assert.Nil(t, err)

	data := []registry.Pair{
		{
			Key:   nb.Key(),
			Value: nbBytes,
		},
	}

	task.(*SubscriptionTask).watchCh <- data
	task.(*SubscriptionTask).watchCh <- nil
	close(task.(*SubscriptionTask).watchCh)
	cancel()
	<-doneCh
	assert.Fail(t, "test")
}
