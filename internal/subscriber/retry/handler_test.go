// +build unit

package retry

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/razorpay/metro/internal/brokerstore/mocks"
	"github.com/razorpay/metro/pkg/messagebroker"
	mocks2 "github.com/razorpay/metro/pkg/messagebroker/mocks"
	"github.com/stretchr/testify/assert"
)

func Test_Handler_Success(t *testing.T) {

	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockBrokerStore := mocks.NewMockIBrokerStore(ctrl)
	mockProducer := mocks2.NewMockProducer(ctrl)

	mockBrokerStore.EXPECT().GetProducer(gomock.AssignableToTypeOf(ctx), gomock.Any()).Return(mockProducer, nil)
	mockProducer.EXPECT().SendMessage(gomock.AssignableToTypeOf(ctx), gomock.Any()).Return(nil, nil)

	handler := NewPushToPrimaryRetryTopicHandler(mockBrokerStore)
	err := handler.Do(ctx, messagebroker.ReceivedMessage{})
	assert.Nil(t, err)
}

func Test_Handler_Failure1(t *testing.T) {

	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockBrokerStore := mocks.NewMockIBrokerStore(ctrl)

	mockBrokerStore.EXPECT().GetProducer(gomock.AssignableToTypeOf(ctx), gomock.Any()).Return(nil, errors.New("failed to create producer"))

	handler := NewPushToPrimaryRetryTopicHandler(mockBrokerStore)
	err := handler.Do(ctx, messagebroker.ReceivedMessage{})
	assert.NotNil(t, err)
}

func Test_Handler_Failure2(t *testing.T) {

	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mockBrokerStore := mocks.NewMockIBrokerStore(ctrl)
	mockProducer := mocks2.NewMockProducer(ctrl)

	mockBrokerStore.EXPECT().GetProducer(gomock.AssignableToTypeOf(ctx), gomock.Any()).Return(mockProducer, nil)
	mockProducer.EXPECT().SendMessage(gomock.AssignableToTypeOf(ctx), gomock.Any()).Return(nil, errors.New("failed to send message to broker"))

	handler := NewPushToPrimaryRetryTopicHandler(mockBrokerStore)
	err := handler.Do(ctx, messagebroker.ReceivedMessage{})
	assert.NotNil(t, err)
}
