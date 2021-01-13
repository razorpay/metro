package leaderelection

import (
	"context"
	"fmt"
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

func TestNewLeaderElectionFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registryMock := mocks.NewMockIRegistry(ctrl)
	config := getConfig()
	config.Path = ""
	c1, err := New(config, registryMock)
	assert.NotNil(t, err)
	assert.Nil(t, c1)
}

func TestLeaderElectionAcquire(t *testing.T) {
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

func TestLeaderElectionAcquireRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registryMock := mocks.NewMockIRegistry(ctrl)
	registryMock.EXPECT().Register("leaderelection-test", 30*time.Second).Return("id", nil).Times(1)
	registryMock.EXPECT().IsRegistered("id").Return(true).Times(1)
	registryMock.EXPECT().Renew("id").Return(nil).Times(1)

	gomock.InOrder(
		registryMock.EXPECT().Acquire("id", "leader/election/test", gomock.Any()).Return(false).Times(1),
		registryMock.EXPECT().Acquire("id", "leader/election/test", gomock.Any()).Return(true).Times(1),
	)

	c1, _ := New(getConfig(), registryMock)
	acquired := c1.tryAcquireOrRenew(context.Background())
	assert.NotNil(t, c1.nodeID)
	assert.Equal(t, false, acquired)

	acquired = c1.tryAcquireOrRenew(context.Background())
	assert.NotNil(t, c1.nodeID)
	assert.Equal(t, true, acquired)
}

func TestLeaderElectionReregister(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registryMock := mocks.NewMockIRegistry(ctrl)
	gomock.InOrder(
		registryMock.EXPECT().Register("leaderelection-test", 30*time.Second).Return("id", nil).Times(1),
		registryMock.EXPECT().Register("leaderelection-test", 30*time.Second).Return("id2", nil).Times(1),
	)
	registryMock.EXPECT().IsRegistered("id").Return(false).Times(1)
	gomock.InOrder(
		registryMock.EXPECT().Acquire("id", "leader/election/test", gomock.Any()).Return(true).Times(1),
		registryMock.EXPECT().Acquire("id2", "leader/election/test", gomock.Any()).Return(true).Times(1),
	)

	c1, _ := New(getConfig(), registryMock)
	acquired := c1.tryAcquireOrRenew(context.Background())
	assert.Equal(t, "id", c1.nodeID)
	assert.Equal(t, true, acquired)

	acquired = c1.tryAcquireOrRenew(context.Background())
	assert.Equal(t, "id2", c1.nodeID)
	assert.Equal(t, true, acquired)
}

func TestLeaderElectionRun(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registryMock := mocks.NewMockIRegistry(ctrl)

	registryMock.EXPECT().Register("leaderelection-test", 30*time.Second).Return("id", nil).Times(1)
	registryMock.EXPECT().IsRegistered("id").Return(true).AnyTimes()
	registryMock.EXPECT().Deregister("id").Return(nil).Times(1)
	registryMock.EXPECT().Acquire("id", "leader/election/test", gomock.Any()).Return(true).AnyTimes()

	config := getConfig()

	config.Callbacks.OnStartedLeading = func(ctx context.Context) error {
		return fmt.Errorf("terminate leader election")
	}
	c, _ := New(config, registryMock)
	assert.NotNil(t, c)

	// run leader election, it should call only expected registry calls as defined above
	c.Run(context.Background())

	assert.Equal(t, "", c.nodeID)
	assert.Equal(t, false, c.IsLeader())
}

func TestDeregisterNodeUnregistered(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registryMock := mocks.NewMockIRegistry(ctrl)
	config := getConfig()

	c, _ := New(config, registryMock)
	assert.NotNil(t, c)

	// there shoudn't be any call to registry since node was not registered
	c.release(context.Background())
	assert.Equal(t, "", c.nodeID)
	assert.Equal(t, false, c.IsLeader())

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
