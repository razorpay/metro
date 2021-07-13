package tasks

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	mocks2 "github.com/razorpay/metro/internal/brokerstore/mocks"
	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/pkg/registry/mocks"
)

func TestScheduleManager_Start(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registryMock := mocks.NewMockIRegistry(ctrl)
	brokerstoreMock := mocks2.NewMockIBrokerStore(ctrl)
	watcherMock := mocks.NewMockIWatcher(ctrl)

	workerID := uuid.New().String()
	manager, err := NewScheduleManager(workerID, registryMock, brokerstoreMock)
	assert.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	// Mock responses to registry
	registryMock.EXPECT().Register(gomock.Any(), "metro/metro-worker", 30*time.Second).Return("id", nil).AnyTimes()
	registryMock.EXPECT().Acquire(gomock.Any(), "id", common.GetBasePrefix()+"nodes/"+workerID, gomock.Any()).Return(true, nil).AnyTimes()
	registryMock.EXPECT().RenewPeriodic(gomock.Any(), "id", 30*time.Second, gomock.Any()).Return(nil).AnyTimes()
	registryMock.EXPECT().Release(gomock.Any(), "id", common.GetBasePrefix()+"leader/election", workerID).Return(true).AnyTimes()
	registryMock.EXPECT().Watch(gomock.Any(), gomock.Any()).Return(watcherMock, nil).AnyTimes()
	registryMock.EXPECT().Exists(gomock.Any(), common.GetBasePrefix()+"nodes/"+workerID).Return(true, nil).AnyTimes()

	// mock watcher
	watcherMock.EXPECT().StartWatch().Do(func() {
		cancel()
	}).Return(nil)
	watcherMock.EXPECT().StopWatch().Return()

	err = manager.Run(ctx)
	assert.NotNil(t, err)
}

func TestScheduleManager_lead(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registryMock := mocks.NewMockIRegistry(ctrl)
	brokerstoreMock := mocks2.NewMockIBrokerStore(ctrl)
	watcherMock := mocks.NewMockIWatcher(ctrl)

	workerID := uuid.New().String()
	manager, err := NewScheduleManager(workerID, registryMock, brokerstoreMock)
	assert.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	// mock registry
	registryMock.EXPECT().Watch(gomock.Any(), gomock.Any()).Return(watcherMock, nil).AnyTimes()

	// mock watcher
	watcherMock.EXPECT().StartWatch().Do(func() {}).Return(nil).AnyTimes()
	watcherMock.EXPECT().StopWatch().Return().AnyTimes()

	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)

		err = manager.(*ScheduleManager).lead(ctx)
		assert.Equal(t, err, context.Canceled)
	}()

	time.Sleep(time.Second * 1)
	cancel()
	<-doneCh
}
