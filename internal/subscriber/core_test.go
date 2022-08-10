package subscriber

import (
	"context"

	"github.com/golang/mock/gomock"
	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/brokerstore/mocks"
	"github.com/razorpay/metro/internal/credentials"
	"github.com/razorpay/metro/internal/offset"
	oCore "github.com/razorpay/metro/internal/offset/mocks/core"
	"github.com/razorpay/metro/internal/subscription"
	sCore "github.com/razorpay/metro/internal/subscription/mocks/core"
	"github.com/razorpay/metro/pkg/cache"
	mCache "github.com/razorpay/metro/pkg/cache/mocks"
	"github.com/razorpay/metro/pkg/messagebroker"
	mockMB "github.com/razorpay/metro/pkg/messagebroker/mocks"
	"github.com/stretchr/testify/assert"

	"testing"
)

func TestCore_NewSubscriber(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBrokerStoreCore := mocks.NewMockIBrokerStore(ctrl)
	mockMessageBroker := mockMB.NewMockBroker(ctrl)
	mockSubscriptionCore := sCore.NewMockICore(ctrl)
	mockOffsetCore := oCore.NewMockICore(ctrl)
	ch := mCache.NewMockICache(ctrl)
	ackCh := make(chan *AckMessage)
	modAckCh := make(chan *ModAckMessage)
	requestCh := make(chan *PullRequest)
	ctx := context.Background()
	sub := subscription.Model{
		Name:  subName,
		Topic: topicName,
		PushConfig: &subscription.PushConfig{
			PushEndpoint: "https://www.razorpay.com/api",
			Attributes: map[string]string{
				"test_key": "test_value",
			},
			Credentials: &credentials.Model{
				Username: "test_user",
				Password: "",
			},
		},
		DeadLetterPolicy:               nil,
		RetryPolicy:                    nil,
		AckDeadlineSeconds:             10,
		ExtractedTopicProjectID:        "project123",
		ExtractedTopicName:             topicName,
		ExtractedSubscriptionName:      subName,
		ExtractedSubscriptionProjectID: "project123",
	}
	cs := getMockConsumerCore(ctx, ctrl)
	type fields struct {
		bs               brokerstore.IBrokerStore
		subscriptionCore subscription.ICore
		offsetCore       offset.ICore
		ch               cache.ICache
	}
	type args struct {
		ctx                    context.Context
		subscriberID           string
		subscription           *subscription.Model
		timeoutInMs            int
		maxOutstandingMessages int64
		maxOutstandingBytes    int64
		requestCh              chan *PullRequest
		ackCh                  chan *AckMessage
		modAckCh               chan *ModAckMessage
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Create new subscriber basicImpl",
			fields: fields{
				bs:               mockBrokerStoreCore,
				subscriptionCore: mockSubscriptionCore,
				offsetCore:       mockOffsetCore,
				ch:               ch,
			},
			args: args{
				ctx:                    ctx,
				subscriberID:           subID,
				subscription:           &sub,
				timeoutInMs:            1000,
				maxOutstandingMessages: 1000,
				maxOutstandingBytes:    1000,
				requestCh:              requestCh,
				ackCh:                  ackCh,
				modAckCh:               modAckCh,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockBrokerStoreCore.EXPECT().GetConsumer(
				gomock.Any(),
				gomock.Any(),
			).Return(cs, nil)
			mockMessageBroker.EXPECT().Resume(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
			ch.EXPECT().Get(gomock.Any(), gomock.Any()).Return([]byte{'0'}, nil).AnyTimes()
			ch.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			c := &Core{
				bs:               tt.fields.bs,
				subscriptionCore: tt.fields.subscriptionCore,
				offsetCore:       tt.fields.offsetCore,
				ch:               tt.fields.ch,
			}
			_, err := c.NewSubscriber(tt.args.ctx, tt.args.subscriberID, tt.args.subscription, tt.args.timeoutInMs, tt.args.maxOutstandingMessages, tt.args.maxOutstandingBytes, tt.args.requestCh, tt.args.ackCh, tt.args.modAckCh)
			if err != nil && !tt.wantErr {
				t.Errorf("NewSubscriber Error %v", err)
			}
		})
	}
}

func TestNewCore(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBrokerStoreCore := mocks.NewMockIBrokerStore(ctrl)
	mockSubscriptionCore := sCore.NewMockICore(ctrl)
	mockOffsetCore := oCore.NewMockICore(ctrl)
	mockCache := mCache.NewMockICache(ctrl)

	type args struct {
		bs               brokerstore.IBrokerStore
		subscriptionCore subscription.ICore
		offsetCore       offset.ICore
		ch               cache.ICache
	}
	tests := []struct {
		name string
		args args
		want ICore
	}{
		{
			name: "Get New subscriber Core",
			args: args{
				bs:               mockBrokerStoreCore,
				subscriptionCore: mockSubscriptionCore,
				offsetCore:       mockOffsetCore,
				ch:               mockCache,
			},
			want: &Core{
				bs:               mockBrokerStoreCore,
				subscriptionCore: mockSubscriptionCore,
				offsetCore:       mockOffsetCore,
				ch:               mockCache,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, NewCore(tt.args.bs, tt.args.subscriptionCore, tt.args.offsetCore, tt.args.ch), "NewCore(%v, %v, %v, %v)", tt.args.bs, tt.args.subscriptionCore, tt.args.offsetCore, tt.args.ch)
		})
	}
}

func getMockConsumerCore(ctx context.Context, ctrl *gomock.Controller) *mockMB.MockConsumer {
	cs := mockMB.NewMockConsumer(ctrl)
	req := messagebroker.GetTopicMetadataRequest{
		Topic:     topicName,
		Partition: partition,
	}
	cs.EXPECT().GetTopicMetadata(ctx, req).Return(messagebroker.GetTopicMetadataResponse{}, nil).AnyTimes()
	cs.EXPECT().Pause(ctx, gomock.Any()).AnyTimes().Return(nil)
	cs.EXPECT().CommitByPartitionAndOffset(gomock.Any(), gomock.Any()).Return(
		messagebroker.CommitOnTopicResponse{}, nil,
	).AnyTimes()
	return cs
}
