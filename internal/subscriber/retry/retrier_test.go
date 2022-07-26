// +build unit

package retry

import (
	"context"
	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/brokerstore/mocks"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/internal/topic"
	"github.com/razorpay/metro/pkg/cache"
	mocks2 "github.com/razorpay/metro/pkg/cache/mocks"
	"github.com/razorpay/metro/pkg/messagebroker"
	mocks3 "github.com/razorpay/metro/pkg/messagebroker/mocks"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

type intervalTest struct {
	min, max                         int
	currentRetryCount, maxRetryCount int
	currentInterval                  int
	want                             []float64
}

const (
	delayTopic string = "projects/project123/topics/subscription-name.delay.5.seconds"
	topicName  string = "primary-topic"
	retryTopic string = "retry-topic"
	partition  int32  = 0
)

func Test_Retrier(t *testing.T) {

}

func Test_Interval_Calculator(t *testing.T) {

	tests := []intervalTest{
		{
			min:               10,
			max:               600,
			currentRetryCount: 1,
			maxRetryCount:     5,
			currentInterval:   0,
			want:              []float64{30, 60, 150, 300, 600},
		},
		{
			min:               1,
			max:               5,
			currentRetryCount: 1,
			maxRetryCount:     5,
			currentInterval:   0,
			want:              []float64{5, 5, 5, 5, 5},
		},
		{
			min:               30,
			max:               150,
			currentRetryCount: 1,
			maxRetryCount:     4,
			currentInterval:   0,
			want:              []float64{30, 150, 150, 150},
		},
		{
			min:               300,
			max:               600,
			currentRetryCount: 1,
			maxRetryCount:     4,
			currentInterval:   0,
			want:              []float64{300, 600, 600, 600},
		},
		{
			min:               600,
			max:               600,
			currentRetryCount: 1,
			maxRetryCount:     5,
			currentInterval:   0,
			want:              []float64{600, 600, 600, 600, 600},
		},
		{
			min:               600,
			max:               3600,
			currentRetryCount: 1,
			maxRetryCount:     7,
			currentInterval:   0,
			want:              []float64{600, 1800, 3600, 3600, 3600, 3600, 3600},
		},
		{
			min:               1800,
			max:               3600,
			currentRetryCount: 1,
			maxRetryCount:     3,
			currentInterval:   0,
			want:              []float64{1800, 3600, 3600},
		},
	}

	availableDelayIntervals := topic.Intervals
	for _, test := range tests {
		actualIntervals := findAllRetryIntervals(test.min, test.max, test.currentRetryCount, test.maxRetryCount, test.currentInterval, availableDelayIntervals)
		assert.Equal(t, test.want, actualIntervals)
	}
}

func TestRetrier_Start(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBrokerStore := mocks.NewMockIBrokerStore(ctrl)
	mockCache := mocks2.NewMockICache(ctrl)
	ctx := context.Background()
	ctxWt, cancel := context.WithTimeout(ctx, time.Second*2)
	defer cancel()
	dcs := getMockDelayConsumerCore(ctx, ctrl)
	type fields struct {
		subscriberID   string
		subs           *subscription.Model
		bs             brokerstore.IBrokerStore
		ch             cache.ICache
		backoff        Backoff
		finder         IntervalFinder
		handler        MessageHandler
		delayConsumers sync.Map
		errChan        chan error
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "Start a Retrier",
			fields: fields{
				subscriberID:   "subscription-id",
				subs:           getMockSubscriptionModel(),
				bs:             mockBrokerStore,
				ch:             mockCache,
				backoff:        NewFixedWindowBackoff(),
				finder:         NewClosestIntervalWithCeil(),
				handler:        NewPushToPrimaryRetryTopicHandler(mockBrokerStore),
				delayConsumers: sync.Map{},
				errChan:        make(chan error),
			},
			args: args{ctxWt},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockBrokerStore.EXPECT().GetConsumer(
				gomock.Any(),
				gomock.Any(),
			).Return(dcs, nil).AnyTimes()
			r := &Retrier{
				subscriberID:   tt.fields.subscriberID,
				subs:           tt.fields.subs,
				bs:             tt.fields.bs,
				ch:             tt.fields.ch,
				backoff:        tt.fields.backoff,
				finder:         tt.fields.finder,
				handler:        tt.fields.handler,
				delayConsumers: tt.fields.delayConsumers,
				errChan:        tt.fields.errChan,
			}
			err := r.Start(tt.args.ctx)
			assert.Nil(t, err)
		})
	}
}

func getMockDelayConsumerCore(ctx context.Context, ctrl *gomock.Controller) *mocks3.MockConsumer {
	dcs := mocks3.NewMockConsumer(ctrl)
	req := messagebroker.GetTopicMetadataRequest{
		Topic:     delayTopic,
		Partition: partition,
	}
	dcs.EXPECT().GetTopicMetadata(ctx, req).Return(messagebroker.GetTopicMetadataResponse{}, nil).AnyTimes()
	dcs.EXPECT().Pause(ctx, gomock.Any()).AnyTimes().Return(nil)
	dcs.EXPECT().CommitByPartitionAndOffset(gomock.Any(), gomock.Any()).Return(
		messagebroker.CommitOnTopicResponse{}, nil,
	).AnyTimes()
	dcs.EXPECT().Resume(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	messages := make([]messagebroker.ReceivedMessage, 0, 3)
	data, _ := proto.Marshal(&metrov1.PubsubMessage{Data: []byte("test")})
	msgProto := messagebroker.ReceivedMessage{
		Data:      data,
		Topic:     topicName,
		Partition: partition,
	}
	msgProto.MessageID = "1"
	messages = append(messages, msgProto)
	dcs.EXPECT().ReceiveMessages(gomock.Any(), gomock.Any()).Return(
		&messagebroker.GetMessagesFromTopicResponse{
			Messages: messages,
		}, nil,
	).AnyTimes()
	return dcs
}
