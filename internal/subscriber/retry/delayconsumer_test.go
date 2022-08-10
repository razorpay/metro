//go:build unit
// +build unit

package retry

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/razorpay/metro/internal/brokerstore/mocks"
	"github.com/razorpay/metro/internal/credentials"
	mocks3 "github.com/razorpay/metro/internal/subscriber/retry/mocks"
	"github.com/razorpay/metro/internal/subscription"
	cachemock "github.com/razorpay/metro/pkg/cache/mocks"
	"github.com/razorpay/metro/pkg/messagebroker"
	mocks2 "github.com/razorpay/metro/pkg/messagebroker/mocks"
	"github.com/stretchr/testify/assert"
)

func TestDelayConsumer_Run(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	ctrl := gomock.NewController(t)
	mockBrokerStore := mocks.NewMockIBrokerStore(ctrl)
	mockHandler := mocks3.NewMockMessageHandler(ctrl)
	mockConsumer := mocks2.NewMockConsumer(ctrl)
	cache := cachemock.NewMockICache(ctrl)

	subs := getDummySubscriptionModel()
	subscriberID := "subscriber-id"

	mockBrokerStore.EXPECT().GetConsumer(gomock.Any(), gomock.Any()).Return(mockConsumer, nil)
	mockConsumer.EXPECT().Resume(gomock.AssignableToTypeOf(ctx), gomock.Any()).Return(nil).AnyTimes()

	// initialize delay consumer
	dc, err := NewDelayConsumer(ctx, subscriberID, "t1", subs, mockBrokerStore, mockHandler, cache, make(chan error))
	assert.NotNil(t, dc)
	assert.Nil(t, err)
	assert.NotNil(t, dc.LogFields())

	msgs := make([]messagebroker.ReceivedMessage, 0)
	msgs = append(msgs, getDummyBrokerMessage())
	resp := &messagebroker.GetMessagesFromTopicResponse{Messages: msgs}
	mockConsumer.EXPECT().ReceiveMessages(gomock.Any(), gomock.Any()).Return(resp, nil).AnyTimes()
	cache.EXPECT().Get(gomock.Any(), gomock.Any()).Return([]byte{'0'}, nil).AnyTimes()
	cache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	// on new message from broker
	mockHandler.EXPECT().Do(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockConsumer.EXPECT().CommitByPartitionAndOffset(gomock.Any(), gomock.Any()).Return(messagebroker.CommitOnTopicResponse{}, nil).AnyTimes()

	// on context cancellation
	mockBrokerStore.EXPECT().RemoveConsumer(gomock.Any(), gomock.Any()).Return(true)
	mockConsumer.EXPECT().Close(gomock.Any()).Return(nil)

	// running the consumer with a deterministic timeout
	dc.Run(ctx)
}

func TestDelayConsumer_Pause_Resume(t *testing.T) {
	ctx := context.Background()
	subCtx, cancel := context.WithCancel(ctx)

	ctrl := gomock.NewController(t)
	mockBrokerStore := mocks.NewMockIBrokerStore(ctrl)
	mockHandler := mocks3.NewMockMessageHandler(ctrl)
	mockConsumer := mocks2.NewMockConsumer(ctrl)
	cache := cachemock.NewMockICache(ctrl)

	subs := getDummySubscriptionModel()
	subscriberID := "subscriber-id"
	mockBrokerStore.EXPECT().GetConsumer(gomock.Any(), gomock.Any()).Return(mockConsumer, nil)
	mockConsumer.EXPECT().Resume(gomock.AssignableToTypeOf(subCtx), gomock.Any()).Return(nil).AnyTimes()
	mockConsumer.EXPECT().Pause(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	// initialize delay consumer
	errCh := make(chan error)
	dc, err := NewDelayConsumer(subCtx, subscriberID, "t1", subs, mockBrokerStore, mockHandler, cache, errCh)
	producer := buildMockProducer(ctx, ctrl)
	assert.NotNil(t, dc)
	assert.Nil(t, err)
	assert.NotNil(t, dc.LogFields())
	tFut := time.Now().Add(time.Second * 10)
	msgs := make([]messagebroker.ReceivedMessage, 0)
	msg := getDummyBrokerMessage()
	msg.NextDeliveryTime = tFut
	msgs = append(msgs, msg)
	resp := &messagebroker.GetMessagesFromTopicResponse{Messages: msgs}
	count := 0
	mockConsumer.EXPECT().ReceiveMessages(gomock.Any(), gomock.Any()).DoAndReturn(
		func(arg0 context.Context, arg1 messagebroker.GetMessagesFromTopicRequest) (*messagebroker.GetMessagesFromTopicResponse, error) {
			if count == 0 {
				count++
				return resp, nil
			} else {
				return &messagebroker.GetMessagesFromTopicResponse{}, nil
			}
		}).AnyTimes()
	cache.EXPECT().Get(gomock.Any(), gomock.Any()).Return([]byte{'0'}, nil).AnyTimes()
	cache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	// on new message from broker
	mockHandler.EXPECT().Do(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockConsumer.EXPECT().CommitByPartitionAndOffset(gomock.Any(), gomock.Any()).Return(messagebroker.CommitOnTopicResponse{}, nil).AnyTimes()
	mockConsumer.EXPECT().Resume(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockBrokerStore.EXPECT().RemoveConsumer(gomock.Any(), gomock.Any()).Return(true)
	mockBrokerStore.EXPECT().GetProducer(gomock.Any(), gomock.Any()).Return(producer, nil).AnyTimes()
	mockConsumer.EXPECT().Close(gomock.Any()).Return(nil)
	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name:    "Test Delay Consumer Run Pause Resume",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			go dc.Run(ctx)
			select {
			case err := <-errCh:
				if !tt.wantErr {
					t.Errorf("Got Error : %v", err)
				}
			case <-time.NewTicker(time.Second * 1).C:
				assert.Equal(t, len(dc.cachedMsgs), 0)
				cancel()
				<-time.NewTicker(time.Second * 1).C
			}
		})
	}
}

func TestDelayConsumer_deadLetter(t *testing.T) {
	ctx := context.Background()
	subCtx, cancel := context.WithCancel(ctx)
	ctrl := gomock.NewController(t)
	mockBrokerStore := mocks.NewMockIBrokerStore(ctrl)
	mockHandler := mocks3.NewMockMessageHandler(ctrl)
	mockConsumer := mocks2.NewMockConsumer(ctrl)
	cache := cachemock.NewMockICache(ctrl)

	subs := getDummySubscriptionModel()
	subscriberID := "subscriber-dl-id"
	mockBrokerStore.EXPECT().GetConsumer(gomock.Any(), gomock.Any()).Return(mockConsumer, nil)
	mockConsumer.EXPECT().Resume(gomock.AssignableToTypeOf(subCtx), gomock.Any()).Return(nil).AnyTimes()
	mockConsumer.EXPECT().Pause(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	// initialize delay consumer
	errCh := make(chan error)
	dc, err := NewDelayConsumer(subCtx, subscriberID, "t1-dl", subs, mockBrokerStore, mockHandler, cache, errCh)
	producer := buildMockProducer(ctx, ctrl)
	assert.NotNil(t, dc)
	assert.Nil(t, err)
	assert.NotNil(t, dc.LogFields())
	tFut := time.Now().Add(time.Second * 10)
	msgs := make([]messagebroker.ReceivedMessage, 0)
	msg := getDummyBrokerMessageDLQ()
	msg.NextDeliveryTime = tFut
	msgs = append(msgs, msg)
	resp := &messagebroker.GetMessagesFromTopicResponse{Messages: msgs}
	count := 0
	mockConsumer.EXPECT().ReceiveMessages(gomock.Any(), gomock.Any()).DoAndReturn(
		func(arg0 context.Context, arg1 messagebroker.GetMessagesFromTopicRequest) (*messagebroker.GetMessagesFromTopicResponse, error) {
			if count == 0 {
				count++
				return resp, nil
			} else {
				return &messagebroker.GetMessagesFromTopicResponse{}, nil
			}
		}).AnyTimes()
	cache.EXPECT().Get(gomock.Any(), gomock.Any()).Return([]byte{'0'}, nil).AnyTimes()
	cache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	// on new message from broker
	mockHandler.EXPECT().Do(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockConsumer.EXPECT().CommitByPartitionAndOffset(gomock.Any(), gomock.Any()).Return(messagebroker.CommitOnTopicResponse{}, nil).AnyTimes()
	mockBrokerStore.EXPECT().RemoveConsumer(gomock.Any(), gomock.Any()).Return(true)
	mockBrokerStore.EXPECT().GetProducer(gomock.Any(), gomock.Any()).Return(producer, nil).AnyTimes()
	mockConsumer.EXPECT().Close(gomock.Any()).Return(nil)
	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name:    "Test Delay Consumer Run Pause Resume",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			go dc.Run(ctx)
			select {
			case err := <-errCh:
				t.Errorf("Got Error : %v", err)
			case <-time.NewTicker(time.Second * 2).C:
				cancel()
				<-time.NewTicker(time.Second * 1).C
			}
		})
	}
}

func getDummyBrokerMessage() messagebroker.ReceivedMessage {
	tNow := time.Now()
	tPast := tNow.Add(time.Second * 100 * -1)
	return messagebroker.ReceivedMessage{
		Data:       bytes.NewBufferString("abc").Bytes(),
		Topic:      "t1",
		Partition:  10,
		Offset:     234,
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
func getDummyBrokerMessageDLQ() messagebroker.ReceivedMessage {
	tNow := time.Now()
	tPast := tNow.Add(time.Second * 100 * -1)
	return messagebroker.ReceivedMessage{
		Data:       bytes.NewBufferString("abc").Bytes(),
		Topic:      "t1",
		Partition:  10,
		Offset:     234,
		Attributes: nil,
		MessageHeader: messagebroker.MessageHeader{
			MessageID:            "m1",
			PublishTime:          tNow,
			SourceTopic:          "st1",
			RetryTopic:           "rt1",
			Subscription:         "s1",
			CurrentRetryCount:    2,
			MaxRetryCount:        1,
			CurrentTopic:         "ct1",
			InitialDelayInterval: 10,
			CurrentDelayInterval: 90,
			ClosestDelayInterval: 150,
			DeadLetterTopic:      "dlt1",
			NextDeliveryTime:     tPast,
		},
	}
}

func TestDelayConsumer_retryCount(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	ctrl := gomock.NewController(t)
	mockBrokerStore := mocks.NewMockIBrokerStore(ctrl)
	mockHandler := mocks3.NewMockMessageHandler(ctrl)
	mockConsumer := mocks2.NewMockConsumer(ctrl)
	cache := cachemock.NewMockICache(ctrl)

	subs := getDummySubscriptionModel()
	subscriberID := "subscriber-id"

	mockBrokerStore.EXPECT().GetConsumer(gomock.Any(), gomock.Any()).Return(mockConsumer, nil)
	mockConsumer.EXPECT().Resume(gomock.AssignableToTypeOf(ctx), gomock.Any()).Return(nil).AnyTimes()

	// initialize delay consumer
	dc, err := NewDelayConsumer(ctx, subscriberID, "t1", subs, mockBrokerStore, mockHandler, cache, make(chan error))
	assert.NotNil(t, dc)
	assert.Nil(t, err)
	assert.NotNil(t, dc.LogFields())

	msgs := make([]messagebroker.ReceivedMessage, 0)
	resp := &messagebroker.GetMessagesFromTopicResponse{Messages: msgs}
	mockConsumer.EXPECT().ReceiveMessages(gomock.Any(), gomock.Any()).Return(resp, nil).AnyTimes()
	cache.EXPECT().Get(gomock.Any(), gomock.Any()).Return([]byte{'1'}, nil).AnyTimes()
	cache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	cache.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	// on new message from broker
	mockHandler.EXPECT().Do(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockConsumer.EXPECT().CommitByPartitionAndOffset(gomock.Any(), gomock.Any()).Return(messagebroker.CommitOnTopicResponse{}, nil).AnyTimes()
	// on context cancellation
	mockBrokerStore.EXPECT().RemoveConsumer(gomock.Any(), gomock.Any()).Return(true)
	mockConsumer.EXPECT().Close(gomock.Any()).Return(nil)
	tests := []struct {
		name string
	}{
		{
			name: "Test resume delay Consumer",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dc.Run(ctx)
			rc, err := dc.fetchRetryCount(getDummyBrokerMessage())
			assert.Equal(t, rc, 1)
			assert.Nil(t, err)
			err = dc.deleteRetryCount(getDummyBrokerMessage())
			assert.Nil(t, err)
			dc.fetchRetryCount(getDummyBrokerMessage())
			err = dc.incrementRetryCount(getDummyBrokerMessage())
			assert.Nil(t, err)
		})
	}
}

func getDummySubscriptionModel() *subscription.Model {
	return &subscription.Model{
		Name:                           "projects/test-project/subscriptions/test-subscription",
		Labels:                         map[string]string{"label": "value"},
		ExtractedSubscriptionProjectID: "test-project",
		ExtractedTopicProjectID:        "test-project",
		ExtractedTopicName:             "test-topic",
		ExtractedSubscriptionName:      "test-subscription",
		Topic:                          "projects/test-project/topics/test-topic",
		DeadLetterPolicy: &subscription.DeadLetterPolicy{
			DeadLetterTopic:     "projects/test-project/topics/test-subscription-dlq",
			MaxDeliveryAttempts: 10,
		},
		PushConfig: &subscription.PushConfig{
			PushEndpoint: "www.some-url.com",
			Credentials: &credentials.Model{
				Username:  "u",
				Password:  "p",
				ProjectID: "dummy1",
			},
		},
	}
}
func buildMockProducer(ctx context.Context, ctrl *gomock.Controller) *mocks2.MockProducer {
	producer := mocks2.NewMockProducer(ctrl)
	producer.EXPECT().SendMessage(gomock.Any(), gomock.Any()).Return(&messagebroker.SendMessageToTopicResponse{MessageID: "m1"}, nil).AnyTimes()
	return producer
}
