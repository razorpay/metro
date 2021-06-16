// +build unit

package health

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	mocks2 "github.com/razorpay/metro/pkg/messagebroker/mocks"
	"github.com/razorpay/metro/pkg/registry/mocks"

	"github.com/stretchr/testify/assert"
)

func TestHealth_MarkUnhealthy(t *testing.T) {
	ctx := context.Background()
	c, err := NewCore()
	assert.Nil(t, err)
	assert.Equal(t, true, c.IsHealthy(ctx))
	c.MarkUnhealthy()
	assert.Equal(t, false, c.IsHealthy(ctx))
}

func TestHealth_Healthy(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	registryMock := mocks.NewMockIRegistry(ctrl)
	brokerMock := mocks2.NewMockAdmin(ctrl)

	registryMock.EXPECT().IsAlive(gomock.Any()).Return(true, nil)
	brokerMock.EXPECT().IsHealthy(gomock.Any()).Return(true, nil)

	h1 := NewRegistryHealthChecker("consul", registryMock)
	h2 := NewBrokerHealthChecker("kafka", brokerMock)
	c, _ := NewCore(h1, h2)

	assert.True(t, c.IsHealthy(ctx))
}

func TestHealth_UnHealthy1(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	registryMock := mocks.NewMockIRegistry(ctrl)

	registryMock.EXPECT().IsAlive(gomock.Any()).Return(false, nil)
	h1 := NewRegistryHealthChecker("consul", registryMock)
	c, _ := NewCore(h1)

	assert.False(t, c.IsHealthy(ctx))
}

func TestHealth_UnHealthy2(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	brokerMock := mocks2.NewMockAdmin(ctrl)

	brokerMock.EXPECT().IsHealthy(gomock.Any()).Return(false, nil)

	h2 := NewBrokerHealthChecker("kafka", brokerMock)
	c, _ := NewCore(h2)

	assert.False(t, c.IsHealthy(ctx))
}
