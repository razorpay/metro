package subscriber

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/razorpay/metro/internal/subscriber/retry"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/messagebroker"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/stretchr/testify/assert"
)

func TestSubscriber_MessageFiltering(t *testing.T) {
	ctx := context.Background()

	messages := []*metrov1.ReceivedMessage{
		{
			Message: &metrov1.PubsubMessage{
				Attributes: map[string]string{
					"domain": "com",
				},
				MessageId: "1",
			},
		}, {
			Message: &metrov1.PubsubMessage{
				Attributes: map[string]string{
					"domain": "com",
					"x":      "org",
				},
				MessageId: "2",
			},
		}, {
			Message: &metrov1.PubsubMessage{
				Attributes: map[string]string{
					"domain": "org",
				},
				MessageId: "3",
			},
		}, {
			Message: &metrov1.PubsubMessage{
				Attributes: map[string]string{
					"x": "company",
				},
				MessageId: "4",
			},
		}, {
			Message: &metrov1.PubsubMessage{
				Attributes: map[string]string{
					"x": "dotcom",
				},
				MessageId: "5",
			},
		}, {
			Message: &metrov1.PubsubMessage{
				Attributes: map[string]string{
					"x": "org",
				},
				MessageId: "6",
			},
		}, {
			Message: &metrov1.PubsubMessage{
				MessageId: "7",
			},
		},
	}

	tests := []struct {
		FilterExpression   string
		FilteredMessageIDs map[string]interface{}
	}{
		{
			FilterExpression:   "attributes:domain",
			FilteredMessageIDs: map[string]interface{}{"1": true, "2": true, "3": true},
		},
		{
			FilterExpression:   "attributes:domain AND attributes.x = \"org\"",
			FilteredMessageIDs: map[string]interface{}{"2": true},
		},
		{
			FilterExpression:   "hasPrefix(attributes.domain, \"co\") OR hasPrefix(attributes.x, \"co\")",
			FilteredMessageIDs: map[string]interface{}{"1": true, "2": true, "4": true},
		},
		{
			FilterExpression:   "(attributes:domain AND attributes.domain = \"com\") OR (attributes:x AND NOT hasPrefix(attributes.x,\"co\"))",
			FilteredMessageIDs: map[string]interface{}{"1": true, "2": true, "5": true, "6": true},
		},
	}

	for _, test := range tests {
		mockImpl := &BasicImplementation{
			subscription: &subscription.Model{
				Name:             "test-sub",
				Topic:            "test-topic",
				FilterExpression: test.FilterExpression,
			},
		}
		filteredMessages := filterMessages(ctx, mockImpl, messages, make(chan error))
		expectedMap := test.FilteredMessageIDs
		for _, msg := range filteredMessages {
			assert.NotNil(t, expectedMap[msg.Message.MessageId], fmt.Sprintf("Filter: %s, MesageID: %s", test.FilterExpression, msg.Message.MessageId))
			delete(expectedMap, msg.Message.MessageId)
		}
		assert.Empty(t, expectedMap)
	}

}

func TestSubscriber_Run(t *testing.T) {
	ctx := context.Background()
	_, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()
	ctrl := gomock.NewController(t)
	requestChan := make(chan *PullRequest)
	responseChan := make(chan *metrov1.PullResponse)
	errChan := make(chan error, 1000)
	closeChan := make(chan struct{})
	cs := getMockConsumer(ctx, ctrl)
	consumer := getMockConsumerManager(ctx, ctrl, cs)
	type fields struct {
		subscriber *Subscriber
	}
	subs := &Subscriber{
		subscription:        getMockSubscription(),
		topic:               topicName,
		subscriberID:        subID,
		requestChan:         requestChan,
		responseChan:        responseChan,
		ackChan:             make(chan *AckMessage, 10),
		modAckChan:          make(chan *ModAckMessage, 10),
		deadlineTicker:      time.NewTicker(1 * time.Second),
		healthMonitorTicker: time.NewTicker(500 * time.Millisecond),
		errChan:             errChan,
		closeChan:           closeChan,
		consumer:            consumer,
		cancelFunc:          cancelFunc,
		ctx:                 ctx,
		retrier:             &retry.Retrier{},
		subscriberImpl:      getMockBasicImplementation(ctx, consumer, ctrl),
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name             string
		fields           fields
		args             args
		maxNumOfMessages int32
		expectedMsg      []string
	}{
		{
			name: "Test Subscriber Run",
			fields: fields{
				subscriber: subs,
			},
			maxNumOfMessages: 1,
			expectedMsg:      []string{"a", "b"},
			args:             args{ctx: ctx},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs.EXPECT().ReceiveMessages(gomock.Any(), messagebroker.GetMessagesFromTopicRequest{NumOfMessages: tt.maxNumOfMessages, TimeoutMs: 1000}).Return(
				&messagebroker.GetMessagesFromTopicResponse{
					Messages: getMockReceivedMessages(tt.expectedMsg),
				}, nil)
			cs.EXPECT().FetchConsumerLag(gomock.Any()).Return(map[string]uint64{
				"primary-topic=0": 0,
				"retry-topic=0":   0,
			}, nil).AnyTimes()
			sub := tt.fields.subscriber
			go sub.Run(tt.args.ctx)
			<-time.NewTicker(time.Nanosecond * 1000).C
			requestChan <- &PullRequest{
				ctx:              ctx,
				MaxNumOfMessages: 1,
			}
			select {
			case res := <-responseChan:
				assert.NotEmpty(t, res.ReceivedMessages)
				assert.Equal(t, len(tt.expectedMsg), len(res.ReceivedMessages))
			case err := <-errChan:
				t.Errorf("Error Test_Subscriber %v", err)
			case <-time.NewTicker(time.Second * 1).C:
				ctx.Done()
				assert.FailNow(t, "Test case timed out")
			}
		})
	}
}

func getMockSubscription() *subscription.Model {
	return &subscription.Model{
		Name:  subName,
		Topic: topicName,
	}
}
