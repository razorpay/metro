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

	registryMock.EXPECT().Register("leaderelection-test", 30*time.Second).Return("id", nil).MaxTimes(1)
	registryMock.EXPECT().IsRegistered("id").Return(true).AnyTimes()
	registryMock.EXPECT().Deregister("id").Return(nil).MaxTimes(1)
	registryMock.EXPECT().Acquire("id", "leader/election/test", gomock.Any()).Return(false).AnyTimes()

	ctx, cancel := context.WithCancel(context.Background())
	c, _ := New(getConfig(), registryMock)

	var wg sync.WaitGroup

	wg.Add(1)
	go func(c1 *Candidate) error {
		defer wg.Done()
		return c1.Run(ctx)
	}(c)

	cancel()
	wg.Wait()
	assert.Equal(t, "", c.nodeID)
}

func getConfig() Config {
	return Config{
		Path:          "leader/election/test",
		LeaseDuration: 30 * time.Second,
		RetryPeriod:   5 * time.Second,
		RenewDeadline: 20 * time.Second,
		Name:          "leaderelection-test",
		Callbacks: LeaderCallbacks{
			OnStartedLeading: func(gctx context.Context) error {
				return nil
			},
			OnStoppedLeading: func() {

			},
		},
	}
}
