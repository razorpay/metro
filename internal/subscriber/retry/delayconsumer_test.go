//go:build unit
// +build unit

package retry

import (
	"bytes"
	"context"
	"encoding/json"
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

func setupDC(t *testing.T) (
	ctx context.Context,
	ctrl *gomock.Controller,
	subCtx context.Context,
	cancel context.CancelFunc,
	store *mocks.MockIBrokerStore,
	handler *mocks3.MockMessageHandler,
	consumer *mocks2.MockConsumer,
	cache *cachemock.MockICache,
) {
	ctx = context.Background()
	subCtx, cancel = context.WithCancel(ctx)
	ctrl = gomock.NewController(t)
	store = mocks.NewMockIBrokerStore(ctrl)
	handler = mocks3.NewMockMessageHandler(ctrl)
	consumer = mocks2.NewMockConsumer(ctrl)
	cache = cachemock.NewMockICache(ctrl)
	cache.EXPECT().Get(gomock.Any(), gomock.Any()).Return([]byte{'0'}, nil).AnyTimes()
	cache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	handler.EXPECT().Do(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	consumer.EXPECT().CommitByPartitionAndOffset(gomock.Any(), gomock.Any()).Return(messagebroker.CommitOnTopicResponse{}, nil).AnyTimes()
	consumer.EXPECT().Resume(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	consumer.EXPECT().Resume(gomock.AssignableToTypeOf(subCtx), gomock.Any()).Return(nil).AnyTimes()
	consumer.EXPECT().Pause(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	consumer.EXPECT().Close(gomock.Any()).Return(nil)
	store.EXPECT().RemoveConsumer(gomock.Any(), gomock.Any()).Return(true)
	store.EXPECT().GetConsumer(gomock.Any(), gomock.Any()).Return(consumer, nil)
	return
}
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

func TestDelayConsumer_Run_Consume(t *testing.T) {
	ctx, ctrl, subCtx, cancel, store, handler, consumer, cache := setupDC(t)
	subs := getDummySubscriptionModel()
	subscriberID := "subscriber-id"
	errCh := make(chan error)
	dc, err := NewDelayConsumer(subCtx, subscriberID, "t1", subs, store, handler, cache, errCh)
	producer := buildMockProducer(ctx, ctrl)
	store.EXPECT().GetProducer(gomock.Any(), gomock.Any()).Return(producer, nil).AnyTimes()
	assert.NotNil(t, dc)
	assert.Nil(t, err)
	assert.NotNil(t, dc.LogFields())
	msgs := make([]messagebroker.ReceivedMessage, 0)
	msg1 := getDummyBrokerMessage()
	msg1.NextDeliveryTime = time.Now().Add(time.Nanosecond * 1000)
	msg2 := getDummyBrokerMessage()
	msg2.NextDeliveryTime = time.Now().Add(time.Second * 2)
	msgs = append(msgs, msg1, msg2)
	type args struct {
		messages []messagebroker.ReceivedMessage
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "Test Delay Consumer one message counsume out of 2",
			wantErr: false,
			args: args{
				messages: msgs,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count := 0
			consumer.EXPECT().ReceiveMessages(gomock.Any(), gomock.Any()).DoAndReturn(
				func(arg0 context.Context, arg1 messagebroker.GetMessagesFromTopicRequest) (*messagebroker.GetMessagesFromTopicResponse, error) {
					if count == 0 {
						count++
						return &messagebroker.GetMessagesFromTopicResponse{Messages: tt.args.messages}, nil
					} else {
						return &messagebroker.GetMessagesFromTopicResponse{}, nil
					}
				}).AnyTimes()
			go dc.Run(ctx)
			select {
			case err := <-errCh:
				if !tt.wantErr {
					t.Errorf("Got Error : %v", err)
				}
				cancel()
				<-time.NewTicker(time.Millisecond * 1).C
			case <-time.NewTicker(time.Millisecond * 500).C:
				assert.Equal(t, len(dc.cachedMsgs), 1)
				cancel()
				<-time.NewTicker(time.Millisecond * 1).C
			}
		})
	}
}

func TestDelayConsumer_Run_DeadLetter(t *testing.T) {
	ctx, ctrl, subCtx, cancel, store, handler, consumer, cache := setupDC(t)
	subs := getDummySubscriptionModel()
	subscriberID := "subscriber-dl-id"
	errCh := make(chan error)
	dlqTopicChan := make(chan *messagebroker.SendMessageToTopicRequest)
	producer := buildMockDLQProducer(ctx, ctrl, dlqTopicChan)
	store.EXPECT().GetProducer(gomock.Any(), gomock.Any()).Return(producer, nil).AnyTimes()
	dc, err := NewDelayConsumer(subCtx, subscriberID, "t1-dl", subs, store, handler, cache, errCh)
	assert.NotNil(t, dc)
	assert.Nil(t, err)
	assert.NotNil(t, dc.LogFields())
	tFut := time.Now().Add(time.Second * 10)
	msgs := make([]messagebroker.ReceivedMessage, 0)
	msg := getDummyBrokerMessageDLQ()
	msg.NextDeliveryTime = tFut
	msgs = append(msgs, msg)
	count := 0
	type args struct {
		messages []messagebroker.ReceivedMessage
	}
	tests := []struct {
		name     string
		wantErr  bool
		expected *messagebroker.SendMessageToTopicRequest
		args     args
	}{
		{
			name: "Test Delay Consumer msg pushed to DLQ success",
			args: args{
				messages: msgs,
			},
			expected: getMockSendMessageToTopicRequest(),
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			consumer.EXPECT().ReceiveMessages(gomock.Any(), gomock.Any()).DoAndReturn(
				func(arg0 context.Context, arg1 messagebroker.GetMessagesFromTopicRequest) (*messagebroker.GetMessagesFromTopicResponse, error) {
					if count == 0 {
						count++
						return &messagebroker.GetMessagesFromTopicResponse{Messages: tt.args.messages}, nil
					} else {
						return &messagebroker.GetMessagesFromTopicResponse{}, nil
					}
				}).AnyTimes()
			go dc.Run(ctx)
			for {
				select {
				case err := <-errCh:
					t.Errorf("Got Error : %v", err)
				case req := <-dlqTopicChan:
					assert.Equal(t, req, tt.expected)
				case <-time.NewTicker(time.Millisecond * 500).C:
					cancel()
					<-time.NewTicker(time.Millisecond * 1).C
					return
				}
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

func buildMockDLQProducer(ctx context.Context, ctrl *gomock.Controller, dlqTopicChan chan *messagebroker.SendMessageToTopicRequest) *mocks2.MockProducer {
	producer := mocks2.NewMockProducer(ctrl)
	producer.EXPECT().SendMessage(gomock.Any(), gomock.Any()).DoAndReturn(
		func(arg0 context.Context, arg1 messagebroker.SendMessageToTopicRequest) (*messagebroker.SendMessageToTopicResponse, error) {
			dlqTopicChan <- getMockSendMessageToTopicRequest()
			return &messagebroker.SendMessageToTopicResponse{MessageID: "m1"}, nil
		})
	return producer
}

func getMockSendMessageToTopicRequest() *messagebroker.SendMessageToTopicRequest {
	byteMsg, _ := json.Marshal("msg-1")
	return &messagebroker.SendMessageToTopicRequest{
		Topic:     "topic-dlq",
		Message:   byteMsg,
		TimeoutMs: 300,
	}
}
