// +build unit

package leaderelection

import (
	"context"
	"fmt"
	"github.com/razorpay/metro/internal/common"
	"testing"

	"github.com/razorpay/metro/pkg/registry"

	"github.com/golang/mock/gomock"
	"github.com/razorpay/metro/pkg/registry/mocks"
	"github.com/stretchr/testify/assert"
)

func TestNewLeaderElection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registryMock := mocks.NewMockIRegistry(ctrl)
	c1, err := New("node01", "id", getConfig(), registryMock)
	assert.NotNil(t, c1)
	assert.Nil(t, err)
}

func TestNewLeaderElectionFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registryMock := mocks.NewMockIRegistry(ctrl)
	config := getConfig()
	config.LockPath = ""
	c1, err := New("node01", "id", config, registryMock)
	assert.NotNil(t, err)
	assert.Nil(t, c1)
}

func TestLeaderElectionRun(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registryMock := mocks.NewMockIRegistry(ctrl)
	watcherMock := mocks.NewMockIWatcher(ctrl)

	ctx := context.Background()

	registryMock.EXPECT().Acquire(gomock.Any(), "id", common.GetBasePrefix()+"leader/election/test", gomock.Any()).Return(true, nil).AnyTimes()
	registryMock.EXPECT().Release(gomock.Any(), "id", common.GetBasePrefix()+"leader/election/test", gomock.Any()).Return(true).AnyTimes()
	registryMock.EXPECT().Watch(gomock.Any(), gomock.Any()).Return(watcherMock, nil).AnyTimes()

	config := getConfig()

	config.Callbacks.OnStartedLeading = func(ctx context.Context) error {
		return fmt.Errorf("terminate leader election")
	}
	c, _ := New("node01", "id", config, registryMock)
	assert.NotNil(t, c)

	watcherMock.EXPECT().StartWatch().Do(func() {
		data := []registry.Pair{
			{
				Key:       common.GetBasePrefix() + "leader/election/test",
				Value:     []byte("node01"),
				SessionID: "",
			},
		}
		c.handler(context.Background(), data)
	}).Return(nil)

	watcherMock.EXPECT().StopWatch().Return().Times(1)

	// run leader election, it should call only expected registry calls as defined above
	c.Run(ctx)

	assert.Equal(t, "id", c.sessionID)
	assert.Equal(t, true, c.IsLeader())
}

func TestDeregisterNodeUnregistered(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	registryMock := mocks.NewMockIRegistry(ctrl)
	config := getConfig()

	c, _ := New("node01", "id", config, registryMock)
	assert.NotNil(t, c)

	registryMock.EXPECT().Release(gomock.Any(), "id", common.GetBasePrefix()+"leader/election/test", gomock.Any()).Return(true).AnyTimes()
	res := c.release(context.Background())

	assert.Equal(t, true, res)
	assert.Equal(t, "id", c.sessionID)
	assert.Equal(t, false, c.IsLeader())
}

func getConfig() Config {
	return Config{
		LockPath: common.GetBasePrefix() + "leader/election/test",
		Callbacks: LeaderCallbacks{
			OnStartedLeading: func(gctx context.Context) error {
				return nil
			},
			OnStoppedLeading: func(ctx context.Context) {
			},
		},
	}
}
