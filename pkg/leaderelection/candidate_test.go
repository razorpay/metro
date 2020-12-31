package leaderelection

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/razorpay/metro/pkg/registry/mocks"
	"github.com/stretchr/testify/assert"
)

func TestNewLeaderElection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registryMock := mocks.NewMockIRegistry(ctrl)
	c1, err := New(getConfig(), registryMock)
	assert.NotNil(t, c1)
	assert.Nil(t, err)
}

func TestLeaderElectionAcquireOrRenew(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registryMock := mocks.NewMockIRegistry(ctrl)
	registryMock.EXPECT().Register("leaderelection-test", 30*time.Second).Return("id", nil)
	registryMock.EXPECT().Acquire("id", "leader/election/test", gomock.Any()).Return(true)

	c1, _ := New(getConfig(), registryMock)
	acquired := c1.tryAcquireOrRenew(context.Background())

	assert.NotNil(t, c1.nodeID)
	assert.Equal(t, true, acquired)
}

func TestLeaderElectionRun(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registryMock := mocks.NewMockIRegistry(ctrl)

	gomock.InOrder(
		registryMock.EXPECT().Register("leaderelection-test", 30*time.Second).Return("id1", nil).Times(1),
		registryMock.EXPECT().Register("leaderelection-test", 30*time.Second).Return("id2", nil).Times(1),
		registryMock.EXPECT().Register("leaderelection-test", 30*time.Second).Return("id3", nil).Times(1),
	)

	registryMock.EXPECT().IsRegistered("id1").Return(true).AnyTimes()
	registryMock.EXPECT().IsRegistered("id2").Return(true).AnyTimes()
	registryMock.EXPECT().IsRegistered("id3").Return(true).AnyTimes()

	registryMock.EXPECT().Deregister("id1").Return(nil).Times(1)
	registryMock.EXPECT().Deregister("id2").Return(nil).Times(1)
	registryMock.EXPECT().Deregister("id3").Return(nil).Times(1)

	registryMock.EXPECT().Acquire("id1", "leader/election/test", gomock.Any()).Return(false).AnyTimes()
	registryMock.EXPECT().Acquire("id2", "leader/election/test", gomock.Any()).Return(true).AnyTimes()
	registryMock.EXPECT().Acquire("id3", "leader/election/test", gomock.Any()).Return(false).AnyTimes()

	gctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	c1, _ := New(getConfig(), registryMock)
	c2, _ := New(getConfig(), registryMock)
	c3, _ := New(getConfig(), registryMock)

	wg.Add(1)
	go func(wg *sync.WaitGroup) error {
		defer wg.Done()
		return c1.Run(gctx)
	}(&wg)

	wg.Add(1)
	go func(wg *sync.WaitGroup) error {
		defer wg.Done()
		return c2.Run(gctx)
	}(&wg)

	wg.Add(1)
	go func(wg *sync.WaitGroup) error {
		defer wg.Done()
		return c3.Run(gctx)
	}(&wg)

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		for {
			// wait until all nodes are registered
			if c1.nodeID != "" && c2.nodeID != "" && c3.nodeID != "" {
				assert.Equal(t, (c1.nodeID == "id2"), c1.IsLeader())
				assert.Equal(t, (c2.nodeID == "id2"), c2.IsLeader())
				assert.Equal(t, (c3.nodeID == "id2"), c3.IsLeader())
				cancel()
				return
			}
		}
	}(&wg)

	wg.Wait()
}

func getConfig() Config {
	return Config{
		Path:          "leader/election/test",
		LeaseDuration: 30 * time.Second,
		RetryPeriod:   5 * time.Second,
		RenewDeadline: 20 * time.Second,
		Name:          "leaderelection-test",
		Callbacks: LeaderCallbacks{
			OnStartedLeading: func(gctx context.Context) {

			},
			OnStoppedLeading: func() {

			},
		},
	}
}
