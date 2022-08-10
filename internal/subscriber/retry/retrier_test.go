// +build unit

package retry

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/brokerstore/mocks"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/internal/topic"
	"github.com/razorpay/metro/pkg/cache"
	mocks2 "github.com/razorpay/metro/pkg/cache/mocks"
	"github.com/razorpay/metro/pkg/messagebroker"
	mocks3 "github.com/razorpay/metro/pkg/messagebroker/mocks"
	"github.com/stretchr/testify/assert"
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
	ctxWt, cancel := context.WithTimeout(ctx, time.Second*1)
	defer cancel()
	dcs := getMockDelayConsumerCore(ctx, ctrl)
	producer := getMockProducer(ctx, ctrl)
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
		name     string
		fields   fields
		expected []string
		args     args
		wantErr  bool
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
			expected: []string{"projects/test-project/topics/test-subscription.delay.30.seconds", "projects/test-project/topics/test-subscription.delay.5.seconds"},
			args:     args{ctxWt},
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockBrokerStore.EXPECT().GetConsumer(
				gomock.Any(),
				gomock.Any(),
			).Return(dcs, nil).AnyTimes()
			mockBrokerStore.EXPECT().RemoveConsumer(gomock.Any(), gomock.Any()).AnyTimes()
			mockBrokerStore.EXPECT().GetProducer(gomock.Any(), gomock.Any()).Return(producer, nil).AnyTimes()
			mockCache.EXPECT().Get(gomock.Any(), gomock.Any()).Return([]byte{'0'}, nil).AnyTimes()
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
			if !tt.wantErr && err != nil {
				t.Errorf("Error while starting : %v", err)
			}
			dcTopics := []string{}
			r.delayConsumers.Range(func(key, value interface{}) bool {
				dc := value.(*DelayConsumer)
				dcTopics = append(dcTopics, dc.topic)
				return true
			})
			sort.Strings(dcTopics)
			assert.Equal(t, dcTopics, tt.expected)
			err = r.Handle(ctx, getMockBrokerMessage())
			if !tt.wantErr && err != nil {
				t.Errorf("Error while Handelling : %v", err)
			}
			r.Stop(ctx)
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
	dcs.EXPECT().Pause(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
	dcs.EXPECT().CommitByPartitionAndOffset(gomock.Any(), gomock.Any()).Return(
		messagebroker.CommitOnTopicResponse{}, nil,
	).AnyTimes()
	dcs.EXPECT().Close(gomock.Any()).Return(nil).AnyTimes()
	dcs.EXPECT().Resume(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	msgs := make([]messagebroker.ReceivedMessage, 0)
	msg := getMockBrokerMessage()
	msg.NextDeliveryTime = time.Now().Add(time.Second * 3)
	msgs = append(msgs, msg)
	resp := &messagebroker.GetMessagesFromTopicResponse{Messages: msgs}
	dcs.EXPECT().ReceiveMessages(gomock.Any(), gomock.Any()).Return(resp, nil).AnyTimes()
	return dcs
}
func getMockDelayConsumerDLQ(ctx context.Context, ctrl *gomock.Controller) *mocks3.MockConsumer {
	dcs := mocks3.NewMockConsumer(ctrl)
	req := messagebroker.GetTopicMetadataRequest{
		Topic:     delayTopic,
		Partition: partition,
	}
	dcs.EXPECT().GetTopicMetadata(ctx, req).Return(messagebroker.GetTopicMetadataResponse{}, nil).AnyTimes()
	dcs.EXPECT().Pause(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
	dcs.EXPECT().CommitByPartitionAndOffset(gomock.Any(), gomock.Any()).Return(
		messagebroker.CommitOnTopicResponse{}, nil,
	).AnyTimes()
	dcs.EXPECT().Close(gomock.Any()).Return(nil).AnyTimes()
	dcs.EXPECT().Resume(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	msgs := make([]messagebroker.ReceivedMessage, 0)
	msg := getMockBrokerMessageDLQ()
	msg.NextDeliveryTime = time.Now().Add(time.Second * 3)
	msgs = append(msgs, msg)
	resp := &messagebroker.GetMessagesFromTopicResponse{Messages: msgs}
	dcs.EXPECT().ReceiveMessages(gomock.Any(), gomock.Any()).Return(resp, nil).AnyTimes()
	return dcs
}

func getMockBrokerMessageDLQ() messagebroker.ReceivedMessage {
	tNow := time.Now()
	tPast := tNow.Add(time.Second * 100 * -1)
	return messagebroker.ReceivedMessage{
		Data:       bytes.NewBufferString("abc").Bytes(),
		Topic:      "t1",
		Partition:  10,
		Offset:     0,
		Attributes: nil,
		MessageHeader: messagebroker.MessageHeader{
			MessageID:            "m1",
			PublishTime:          tNow,
			SourceTopic:          "st1",
			RetryTopic:           "rt1",
			Subscription:         "s1",
			CurrentRetryCount:    2,
			MaxRetryCount:        10,
			CurrentTopic:         "ct1",
			InitialDelayInterval: 10,
			CurrentDelayInterval: 90,
			ClosestDelayInterval: 150,
			DeadLetterTopic:      "dlt1",
			NextDeliveryTime:     tPast,
		},
	}
}

func getMockBrokerMessage() messagebroker.ReceivedMessage {
	tNow := time.Now()
	tPast := tNow.Add(time.Second * 100 * -1)
	return messagebroker.ReceivedMessage{
		Data:       bytes.NewBufferString("abc").Bytes(),
		Topic:      "t1",
		Partition:  10,
		Offset:     0,
		Attributes: nil,
		MessageHeader: messagebroker.MessageHeader{
			MessageID:            "m1",
			PublishTime:          tNow,
			SourceTopic:          "st1",
			RetryTopic:           "rt1",
			Subscription:         "s1",
			CurrentRetryCount:    2,
			MaxRetryCount:        10,
			CurrentTopic:         "ct1",
			InitialDelayInterval: 10,
			CurrentDelayInterval: 90,
			ClosestDelayInterval: 150,
			DeadLetterTopic:      "dlt1",
			NextDeliveryTime:     tPast,
		},
	}
}

func getMockProducer(ctx context.Context, ctrl *gomock.Controller) *mocks3.MockProducer {
	producer := mocks3.NewMockProducer(ctrl)
	producer.EXPECT().SendMessage(ctx, gomock.Any()).Return(&messagebroker.SendMessageToTopicResponse{MessageID: "m1"}, nil)
	return producer
}
