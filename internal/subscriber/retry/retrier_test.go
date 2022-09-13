//go:build unit
// +build unit

package retry

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/razorpay/metro/internal/brokerstore/mocks"
	"github.com/razorpay/metro/internal/topic"
	mocks2 "github.com/razorpay/metro/pkg/cache/mocks"
	"github.com/razorpay/metro/pkg/messagebroker"
	mocks3 "github.com/razorpay/metro/pkg/messagebroker/mocks"
	"github.com/stretchr/testify/assert"
)

const (
	delayTopic string = "projects/project123/topics/subscription-name.delay.5.seconds"
	topicName  string = "primary-topic"
	retryTopic string = "retry-topic"
	partition  int32  = 0
)

type intervalTest struct {
	min, max                         int
	currentRetryCount, maxRetryCount int
	currentInterval                  int
	want                             []float64
}

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
	ctx := context.Background()
	ctxWt, cancel := context.WithCancel(ctx)
	defer cancel()

	mockBrokerStore := mocks.NewMockIBrokerStore(ctrl)
	mockCache := mocks2.NewMockICache(ctrl)
	mockCache.EXPECT().Get(gomock.AssignableToTypeOf(ctxWt), gomock.Any()).Return([]byte{'0'}, nil).AnyTimes()
	dcs := getMockDelayConsumer(ctx, ctrl)
	producer := getMockProducer(ctx, ctrl)
	mockBrokerStore.EXPECT().GetConsumer(
		ctxWt,
		gomock.Any(),
	).Return(dcs, nil).AnyTimes()
	mockBrokerStore.EXPECT().RemoveConsumer(ctxWt, gomock.Any()).AnyTimes()
	mockBrokerStore.EXPECT().GetProducer(gomock.Any(), gomock.Any()).Return(producer, nil).AnyTimes()

	r := &Retrier{
		subscriberID:   "subscription-id",
		subs:           getMockSubscriptionModel(),
		bs:             mockBrokerStore,
		ch:             mockCache,
		backoff:        NewExponentialWindowBackoff(),
		finder:         NewClosestIntervalWithCeil(),
		handler:        NewPushToPrimaryRetryTopicHandler(mockBrokerStore),
		delayConsumers: sync.Map{},
		errChan:        make(chan error),
	}
	expDelayTopics := r.subs.GetDelayTopics()
	sort.Strings(expDelayTopics)
	err := r.Start(ctxWt)
	assert.Nil(t, err)
	dcTopics := []string{}
	r.delayConsumers.Range(func(key, value interface{}) bool {
		dc := value.(*DelayConsumer)
		dcTopics = append(dcTopics, dc.topic)
		return true
	})
	sort.Strings(dcTopics)
	assert.Equal(t, dcTopics, expDelayTopics)
	err = r.Handle(ctx, getDummyBrokerMessage("msg-1", "m1", time.Now().Add(-time.Second*100), 1))
	assert.Nil(t, err)
	r.Stop(ctx)
}

func getMockDelayConsumer(ctx context.Context, ctrl *gomock.Controller) *mocks3.MockConsumer {
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
	msg := getDummyBrokerMessage("msg-1", "m1", time.Now().Add(-time.Second*100), 5)
	msg.NextDeliveryTime = time.Now().Add(time.Second * 3)
	msgs = append(msgs, msg)
	resp := &messagebroker.GetMessagesFromTopicResponse{Messages: msgs}
	dcs.EXPECT().ReceiveMessages(gomock.Any(), gomock.Any()).Return(resp, nil).AnyTimes()
	return dcs
}
