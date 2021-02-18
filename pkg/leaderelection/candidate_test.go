package leaderelection

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/razorpay/metro/pkg/registry"

	"github.com/golang/mock/gomock"
	"github.com/razorpay/metro/pkg/registry/mocks"
	"github.com/stretchr/testify/assert"
)

func TestNewLeaderElection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registryMock := mocks.NewMockIRegistry(ctrl)
	c1, err := New("node01", getConfig(), registryMock)
	assert.NotNil(t, c1)
	assert.Nil(t, err)
}

func TestNewLeaderElectionFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registryMock := mocks.NewMockIRegistry(ctrl)
	config := getConfig()
	config.LockPath = ""
	c1, err := New("node01", config, registryMock)
	assert.NotNil(t, err)
	assert.Nil(t, c1)
}

func TestLeaderElectionRun(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registryMock := mocks.NewMockIRegistry(ctrl)
	watcherMock := mocks.NewMockIWatcher(ctrl)

	registryMock.EXPECT().Register("leaderelection-test", 30*time.Second).Return("id", nil).Times(1)
	registryMock.EXPECT().IsRegistered("id").Return(true).AnyTimes()
	registryMock.EXPECT().Deregister("id").Return(nil).Times(1)
	registryMock.EXPECT().Acquire("id", "metro/nodes/node01", gomock.Any()).Return(true, nil).AnyTimes()
	registryMock.EXPECT().Acquire("id", "metro/leader/election/test", gomock.Any()).Return(true, nil).AnyTimes()
	registryMock.EXPECT().RenewPeriodic(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	registryMock.EXPECT().Watch(gomock.Any(), gomock.Any()).Return(watcherMock, nil).AnyTimes()

	config := getConfig()

	config.Callbacks.OnStartedLeading = func(ctx context.Context) error {
		return fmt.Errorf("terminate leader election")
	}
	c, _ := New("node01", config, registryMock)
	assert.NotNil(t, c)

	watcherMock.EXPECT().StartWatch().Do(func() {
		data := []registry.Pair{
			{
				Key:       "metro/leader/election/test",
				Value:     []byte("aaa"),
				SessionID: "",
			},
		}
		c.handler(context.Background(), data)
	}).Return(nil)

	watcherMock.EXPECT().StopWatch().Return().Times(1)

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

	c, _ := New("node01", config, registryMock)
	assert.NotNil(t, c)

	// there shoudn't be any call to registry since node was not registered
	c.release(context.Background())
	assert.Equal(t, "", c.nodeID)
	assert.Equal(t, false, c.IsLeader())

}

func getConfig() Config {
	return Config{
		LockPath:      "metro/leader/election/test",
		NodePath:      "metro/nodes/node01",
		LeaseDuration: 30 * time.Second,
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
