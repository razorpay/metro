// +build unit

package tasks

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/razorpay/metro/internal/common"
	mocks3 "github.com/razorpay/metro/internal/node/mocks/core"
	mocks2 "github.com/razorpay/metro/internal/tasks/mocks"
	"github.com/razorpay/metro/pkg/registry/mocks"
)

func TestLeaderTask_Run(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registryMock := mocks.NewMockIRegistry(ctrl)
	watcherMock := mocks.NewMockIWatcher(ctrl)
	nodeCoreMock := mocks3.NewMockICore(ctrl)
	taskMock := mocks2.NewMockITask(ctrl)

	workerID := uuid.New().String()
	task, err := NewLeaderTask(workerID, registryMock, nodeCoreMock, taskMock)
	assert.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Mock responses to registry
	registryMock.EXPECT().Register(gomock.Any(), "metro/metro-worker", 30*time.Second).Return("id", nil).AnyTimes()
	registryMock.EXPECT().RenewPeriodic(gomock.Any(), "id", 30*time.Second, gomock.Any()).Return(nil).AnyTimes()
	registryMock.EXPECT().Release(gomock.Any(), "id", getLeaderElectionKey(), workerID).Return(true)
	registryMock.EXPECT().Watch(gomock.Any(), gomock.Any()).Return(watcherMock, nil).AnyTimes()

	// mock watcher
	watcherMock.EXPECT().StartWatch().Do(func() {
		cancel()
	}).Return(nil)
	watcherMock.EXPECT().StopWatch().Return()

	// mock NodeCore
	nodeCoreMock.EXPECT().AcquireNode(gomock.AssignableToTypeOf(ctx), gomock.Any(), "id").Return(nil)

	err = task.Run(ctx)
	assert.Equal(t, err, context.Canceled)
}

func TestLeaderTask_Run2(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registryMock := mocks.NewMockIRegistry(ctrl)
	watcherMock := mocks.NewMockIWatcher(ctrl)
	nodeCoreMock := mocks3.NewMockICore(ctrl)
	taskMock := mocks2.NewMockITask(ctrl)

	workerID := uuid.New().String()
	task, err := NewLeaderTask(workerID, registryMock, nodeCoreMock, taskMock)
	assert.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Mock responses to registry
	registryMock.EXPECT().Register(gomock.Any(), "metro/metro-worker", 30*time.Second).Return("id", nil).AnyTimes()
	registryMock.EXPECT().RenewPeriodic(gomock.Any(), "id", 30*time.Second, gomock.Any()).Return(nil).AnyTimes()
	registryMock.EXPECT().Release(gomock.Any(), "id", getLeaderElectionKey(), workerID).Return(true)
	registryMock.EXPECT().Watch(gomock.Any(), gomock.Any()).Return(watcherMock, nil).AnyTimes()

	// mock watcher
	watcherMock.EXPECT().StartWatch().Return(nil)
	watcherMock.EXPECT().StopWatch().Return()

	// mock NodeCore
	AcquireErr := errors.New("test")
	nodeCoreMock.EXPECT().AcquireNode(gomock.AssignableToTypeOf(ctx), gomock.Any(), "id").Return(AcquireErr)

	err = task.Run(ctx)
	assert.Equal(t, err, AcquireErr)
}

func TestLeaderTask_lead(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registryMock := mocks.NewMockIRegistry(ctrl)
	nodeCoreMock := mocks3.NewMockICore(ctrl)
	taskMock := mocks2.NewMockITask(ctrl)

	workerID := uuid.New().String()
	task, err := NewLeaderTask(workerID, registryMock, nodeCoreMock, taskMock)
	assert.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// tests
	tests := []struct {
		err error
	}{
		{
			err: nil,
		},
		{
			err: errors.New("test"),
		},
	}

	for _, test := range tests {
		// mock task runs
		taskMock.EXPECT().Run(gomock.AssignableToTypeOf(ctx)).Return(test.err)

		err = task.(*LeaderTask).lead(ctx)
		assert.Equal(t, err, test.err)
	}
}

func TestLeaderTask_getLeaderElectionKey(t *testing.T) {
	key := getLeaderElectionKey()
	assert.Equal(t, key, common.GetBasePrefix()+LeaderKey)
}
