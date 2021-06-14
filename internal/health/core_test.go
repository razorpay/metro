// +build unit

package health

import (
	"testing"

	"github.com/golang/mock/gomock"
	mocks2 "github.com/razorpay/metro/pkg/messagebroker/mocks"
	"github.com/razorpay/metro/pkg/registry/mocks"

	"github.com/stretchr/testify/assert"
)

func TestHealth_MarkUnhealthy(t *testing.T) {
	c, err := NewCore()
	assert.Nil(t, err)
	assert.Equal(t, true, c.IsHealthy())
	c.MarkUnhealthy()
	assert.Equal(t, false, c.IsHealthy())
}

func TestHealth_Healthy(t *testing.T) {
	ctrl := gomock.NewController(t)
	registryMock := mocks.NewMockIRegistry(ctrl)
	brokerMock := mocks2.NewMockAdmin(ctrl)

	registryMock.EXPECT().IsAlive().Return(true, nil)
	brokerMock.EXPECT().IsHealthy().Return(true, nil)

	h1 := NewRegistryHealthChecker("consul", registryMock)
	h2 := NewBrokerHealthChecker("kafka", brokerMock)
	c, _ := NewCore(h1, h2)

	assert.True(t, c.IsHealthy())
}

func TestHealth_UnHealthy1(t *testing.T) {
	ctrl := gomock.NewController(t)
	registryMock := mocks.NewMockIRegistry(ctrl)
	brokerMock := mocks2.NewMockAdmin(ctrl)

	registryMock.EXPECT().IsAlive().Return(false, nil)
	brokerMock.EXPECT().IsHealthy().Return(true, nil)

	h1 := NewRegistryHealthChecker("consul", registryMock)
	h2 := NewBrokerHealthChecker("kafka", brokerMock)
	c, _ := NewCore(h1, h2)

	assert.False(t, c.IsHealthy())
}

func TestHealth_UnHealthy2(t *testing.T) {
	ctrl := gomock.NewController(t)
	registryMock := mocks.NewMockIRegistry(ctrl)
	brokerMock := mocks2.NewMockAdmin(ctrl)

	registryMock.EXPECT().IsAlive().Return(true, nil)
	brokerMock.EXPECT().IsHealthy().Return(false, nil)

	h1 := NewRegistryHealthChecker("consul", registryMock)
	h2 := NewBrokerHealthChecker("kafka", brokerMock)
	c, _ := NewCore(h1, h2)

	assert.False(t, c.IsHealthy())
}
