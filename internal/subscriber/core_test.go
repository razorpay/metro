package subscriber

import (
	"context"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/brokerstore/mocks"
	"github.com/razorpay/metro/internal/offset"
	oCore "github.com/razorpay/metro/internal/offset/mocks/core"
	"github.com/razorpay/metro/internal/subscription"
	sCore "github.com/razorpay/metro/internal/subscription/mocks/core"
	"github.com/razorpay/metro/internal/topic"
	tCore "github.com/razorpay/metro/internal/topic/mocks/core"
	"github.com/razorpay/metro/pkg/cache"
	mCache "github.com/razorpay/metro/pkg/cache/mocks"
	"github.com/razorpay/metro/pkg/messagebroker"
	mockMB "github.com/razorpay/metro/pkg/messagebroker/mocks"
	"github.com/stretchr/testify/assert"

	"reflect"
	"testing"
	"unsafe"
)

const (
	timeoutInMs            = 1000
	maxOutstandingMessages = 1000
	maxOutstandingBytes    = 1000
)

func setupCore(t *testing.T) (
	ctx context.Context,
	cs *mockMB.MockConsumer,
	store *mocks.MockIBrokerStore,
	broker *mockMB.MockBroker,
	core *sCore.MockICore,
	offsetCore *oCore.MockICore,
	cache *mCache.MockICache,
	topicCore *tCore.MockICore,
) {
	ctrl := gomock.NewController(t)
	ctx = context.Background()
	cs = getMockConsumer(ctx, ctrl)
	store = mocks.NewMockIBrokerStore(ctrl)
	broker = mockMB.NewMockBroker(ctrl)
	core = sCore.NewMockICore(ctrl)
	offsetCore = oCore.NewMockICore(ctrl)
	cache = mCache.NewMockICache(ctrl)
	topicCore = tCore.NewMockICore(ctrl)

	cache.EXPECT().Get(gomock.Any(), gomock.Any()).Return([]byte{'0'}, nil).AnyTimes()
	cache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	store.EXPECT().GetProducer(gomock.Any(), gomock.Any()).Return(getMockProducer(ctrl), nil).AnyTimes()
	cs.EXPECT().Close(gomock.Any()).Return(nil).AnyTimes()
	store.EXPECT().RemoveConsumer(gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	topicCore.EXPECT().Get(gomock.Any(), gomock.Any()).AnyTimes()
	return
}

func TestCore_NewSubscriber(t *testing.T) {
	ctx, cs, store, _, core, offsetCore, ch, topicCore := setupCore(t)
	ackCh := make(chan *AckMessage)
	modAckCh := make(chan *ModAckMessage)
	requestCh := make(chan *PullRequest)
	subscrption := getDummySubscriptionWithRetry()
	type fields struct {
		core ICore
	}
	type args struct {
		ctx          context.Context
		subscriberID string
		subscription *subscription.Model
		requestCh    chan *PullRequest
		ackCh        chan *AckMessage
		modAckCh     chan *ModAckMessage
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		expected ISubscriber
		msgs     []string
		wantErr  bool
	}{
		{
			name: "Create new subscriber and run with retry policy",
			fields: fields{
				core: NewCore(store, core, offsetCore, ch, topicCore),
			},
			args: args{
				ctx:          ctx,
				subscriberID: subID,
				subscription: &subscrption,
				requestCh:    requestCh,
				ackCh:        ackCh,
				modAckCh:     modAckCh,
			},
			expected: &Subscriber{
				subscription: &subscrption,
				subscriberID: subID,
			},
			msgs: []string{"a", "b"},
		},
		{
			name: "Create new subscriber with getConsumer error Failure",
			fields: fields{
				core: NewCore(store, core, offsetCore, ch, topicCore),
			},
			args: args{
				ctx:          ctx,
				subscriberID: subID,
				subscription: &subscrption,
				requestCh:    requestCh,
				ackCh:        ackCh,
				modAckCh:     modAckCh,
			},
			expected: nil,
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store.EXPECT().GetConsumer(gomock.Any(), gomock.Any()).
				DoAndReturn(func(arg0 context.Context, arg1 messagebroker.ConsumerClientOptions) (messagebroker.Consumer, error) {
					if tt.wantErr {
						return nil, errors.New("Test Error")
					}
					return cs, nil
				}).AnyTimes()
			count := 0
			cs.EXPECT().ReceiveMessages(gomock.Any(), messagebroker.GetMessagesFromTopicRequest{
				NumOfMessages: 10,
				TimeoutMs:     100,
			}).DoAndReturn(
				func(arg0 context.Context, arg1 messagebroker.GetMessagesFromTopicRequest) (*messagebroker.GetMessagesFromTopicResponse, error) {
					if count == 0 {
						count++
						return &messagebroker.GetMessagesFromTopicResponse{Messages: getMockReceivedMessages(tt.msgs)}, nil
					}
					return &messagebroker.GetMessagesFromTopicResponse{}, nil
				}).AnyTimes()
			sub, err := tt.fields.core.NewSubscriber(tt.args.ctx, tt.args.subscriberID, tt.args.subscription, timeoutInMs,
				maxOutstandingMessages, maxOutstandingBytes, tt.args.requestCh, tt.args.ackCh, tt.args.modAckCh)
			assert.Equal(t, err != nil, tt.wantErr)
			assert.True(t, EqualOnly(sub, tt.expected, []string{"subscription", "subscriberID"}))
			if err == nil {
				sub.Stop()
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
	mockTopicCore := tCore.NewMockICore(ctrl)

	type args struct {
		bs               brokerstore.IBrokerStore
		subscriptionCore subscription.ICore
		offsetCore       offset.ICore
		topicCore        topic.ICore
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
				topicCore:        mockTopicCore,
				ch:               mockCache,
			},
			want: &Core{
				bs:               mockBrokerStoreCore,
				subscriptionCore: mockSubscriptionCore,
				offsetCore:       mockOffsetCore,
				topicCore:        mockTopicCore,
				ch:               mockCache,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewCore(tt.args.bs, tt.args.subscriptionCore, tt.args.offsetCore, tt.args.ch, tt.args.topicCore)
			assert.Equal(t, tt.want, got)
		})
	}
}

func getDummySubscriptionWithRetry() subscription.Model {
	return subscription.Model{
		Name:  subName,
		Topic: topicName,
		DeadLetterPolicy: &subscription.DeadLetterPolicy{
			DeadLetterTopic:     "projects/test-project/topics/test-subscription-dlq",
			MaxDeliveryAttempts: 1,
		},
		RetryPolicy: &subscription.RetryPolicy{
			MinimumBackoff: 5,
			MaximumBackoff: 10,
		},
	}
}

func getMockProducer(ctrl *gomock.Controller) *mockMB.MockProducer {
	producer := mockMB.NewMockProducer(ctrl)
	producer.EXPECT().SendMessage(gomock.Any(), gomock.Any()).Return(&messagebroker.SendMessageToTopicResponse{MessageID: "m1"}, nil).AnyTimes()
	return producer
}

// EqualOnly compares fields defined in the fields array
func EqualOnly(sub1, sub2 interface{}, fields []string) bool {
	if sub1 == nil || sub2 == nil {
		return sub1 == sub2
	}
	val1 := reflect.ValueOf(sub1).Elem()
	val2 := reflect.ValueOf(sub2).Elem()
	for i := 0; i < val1.NumField(); i++ {
		typeField := val1.Type().Field(i)
		if !contains(typeField.Name, fields) {
			continue
		}
		value1 := val1.Field(i)
		value2 := val2.Field(i)
		value1 = reflect.NewAt(value1.Type(), unsafe.Pointer(value1.UnsafeAddr())).Elem()
		value2 = reflect.NewAt(value2.Type(), unsafe.Pointer(value2.UnsafeAddr())).Elem()
		if value1.Interface() != value2.Interface() {
			return false
		}
	}
	return true
}

func contains(name string, fields []string) bool {
	for _, field := range fields {
		if field == name {
			return true
		}
	}
	return false
}
