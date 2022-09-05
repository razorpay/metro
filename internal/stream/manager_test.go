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
	ctx := context.Background()

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
		got, err := NewPushStreamManager(
			ctx,
			uuid.New().String(),
			subName,
			getSubscriptionCoreMock(ctrl, test.wantErr),
			getSubscriberCoreMock(t, ctx, ctrl),
			&httpclient.Config{},
		)
		assert.Equal(t, test.wantErr, err != nil)
		assert.Equal(t, got == nil, test.wantErr)
	}
}

func TestPushStreamManager_Run(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := context.Background()

	psm, err := NewPushStreamManager(
		ctx,
		uuid.New().String(),
		subName,
		getSubscriptionCoreMock(ctrl, false),
		getSubscriberCoreMock(t, ctx, ctrl),
		&httpclient.Config{},
	)
	assert.NoError(t, err)
	assert.NotNil(t, psm)

	psm.Run()
	<-time.NewTicker(1 * time.Second).C

	streamObj := psm.ps
	psm.ps.GetErrorChannel() <- fmt.Errorf("Something went wrong")
	<-time.NewTicker(1 * time.Second).C
	assert.NotNil(t, psm.ps)
	assert.NotEqual(t, psm.ps, streamObj)
}

func TestPushStreamManager_Stop(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := context.Background()

	psm, err := NewPushStreamManager(
		ctx,
		uuid.New().String(),
		subName,
		getSubscriptionCoreMock(ctrl, false),
		getSubscriberCoreMock(t, ctx, ctrl),
		&httpclient.Config{},
	)
	assert.NoError(t, err)
	assert.NotNil(t, psm)
	psm.Run()

	// Stop the stream manager and it should be stopped without any error
	psm.Stop()
	assert.NotNil(t, psm.ctx.Err())
	assert.Equal(t, psm.ctx.Err(), context.Canceled)
}

func getSubscriberCoreMock(t *testing.T, ctx context.Context, ctrl *gomock.Controller) *mocks2.MockICore {
	subscriberCoreMock := mocks2.NewMockICore(ctrl)
	subModel := getMockSubModel("")

	subscriberCoreMock.EXPECT().NewSubscriber(
		ctx,
		gomock.Any(),
		subModel,
		defaultTimeoutMs,
		defaultMaxOutstandingMsgs,
		defaultMaxOuttandingBytes,
		gomock.AssignableToTypeOf(make(chan *subscriber.PullRequest)),
		gomock.AssignableToTypeOf(make(chan *subscriber.AckMessage)),
		gomock.AssignableToTypeOf(make(chan *subscriber.ModAckMessage))).AnyTimes().Return(getMockSubscriber(t, ctx), nil)
	return subscriberCoreMock
}

func getSubscriptionCoreMock(ctrl *gomock.Controller, wantErr bool) *mocks1.MockICore {
	subscriptionCoreMock := mocks1.NewMockICore(ctrl)
	if wantErr {
		subscriptionCoreMock.EXPECT().Get(gomock.Any(), subName).AnyTimes().Return(nil, fmt.Errorf("Something went wrong"))
	} else {
		subscriptionCoreMock.EXPECT().Get(gomock.Any(), subName).AnyTimes().Return(getMockSubModel(""), nil)
	}
	return subscriptionCoreMock
}
