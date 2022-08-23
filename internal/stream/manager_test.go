package stream

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/razorpay/metro/internal/subscriber"
	mocks2 "github.com/razorpay/metro/internal/subscriber/mocks"
	mocks1 "github.com/razorpay/metro/internal/subscription/mocks/core"
	"github.com/razorpay/metro/pkg/httpclient"
	"github.com/stretchr/testify/assert"
)

func TestNewPushStreamManager(t *testing.T) {
	ctrl := gomock.NewController(t)

	tests := []struct {
		wantErr bool
	}{
		{
			wantErr: false,
		},
		{
			wantErr: true,
		},
	}

	for _, test := range tests {
		got, err := getMockPushStreamManager(ctrl, test.wantErr)
		assert.Equal(t, test.wantErr, err != nil)
		assert.Equal(t, got == nil, test.wantErr)
	}
}

func TestPushStreamManager_Run(t *testing.T) {
	ctrl := gomock.NewController(t)
	psm, err := getMockPushStreamManager(ctrl, false)
	assert.NoError(t, err)
	assert.NotNil(t, psm)
	psm.Run()

	// Push an error to errChan of stream and it should still be running
	psm.ps.GetErrorChannel() <- fmt.Errorf("Something went wrong")
	<-time.NewTicker(1 * time.Second).C
	assert.NotNil(t, psm.ps)
}

func TestPushStreamManager_Stop(t *testing.T) {
	ctrl := gomock.NewController(t)
	psm, err := getMockPushStreamManager(ctrl, false)
	assert.NoError(t, err)
	assert.NotNil(t, psm)
	psm.Run()

	// Stop the stream manager and it should be stopped without any error
	psm.Stop()
	assert.NotNil(t, psm.ctx.Err())
}

func getMockPushStreamManager(ctrl *gomock.Controller, wantErr bool) (*PushStreamManager, error) {
	ctx := context.Background()
	subscriptionCoreMock := mocks1.NewMockICore(ctrl)
	subscriberCoreMock := mocks2.NewMockICore(ctrl)
	workerID := uuid.New().String()
	httpConfig := &httpclient.Config{}
	subModel := getMockSubModel("")

	if wantErr {
		subscriptionCoreMock.EXPECT().Get(gomock.Any(), subName).AnyTimes().Return(nil, fmt.Errorf("Something went wrong"))
	} else {
		subscriptionCoreMock.EXPECT().Get(gomock.Any(), subName).AnyTimes().Return(getMockSubModel(""), nil)
	}
	subscriberCoreMock.EXPECT().NewSubscriber(
		ctx,
		workerID,
		subModel,
		defaultTimeoutMs,
		defaultMaxOutstandingMsgs,
		defaultMaxOuttandingBytes,
		gomock.AssignableToTypeOf(make(chan *subscriber.PullRequest)),
		gomock.AssignableToTypeOf(make(chan *subscriber.AckMessage)),
		gomock.AssignableToTypeOf(make(chan *subscriber.ModAckMessage))).AnyTimes().Return(getMockSubscriber(ctx, ctrl), nil)
	return NewPushStreamManager(ctx, workerID, subName, subscriptionCoreMock, subscriberCoreMock, httpConfig)
}
