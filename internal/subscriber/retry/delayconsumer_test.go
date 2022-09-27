//go:build unit
// +build unit

package retry

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
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
	cache.EXPECT().Get(gomock.AssignableToTypeOf(subCtx), gomock.Any()).Return([]byte{'0'}, nil).AnyTimes()
	cache.EXPECT().Set(gomock.AssignableToTypeOf(subCtx), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	handler.EXPECT().Do(gomock.AssignableToTypeOf(subCtx), gomock.Any()).Return(nil).AnyTimes()
	consumer.EXPECT().CommitByPartitionAndOffset(gomock.Any(), gomock.Any()).Return(messagebroker.CommitOnTopicResponse{}, nil).AnyTimes()
	consumer.EXPECT().Resume(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	consumer.EXPECT().Pause(gomock.AssignableToTypeOf(subCtx), messagebroker.PauseOnTopicRequest{
		Topic:     "t1",
		Partition: 0,
	}).Return(nil).AnyTimes()
	consumer.EXPECT().Close(gomock.AssignableToTypeOf(subCtx)).Return(nil).AnyTimes()
	store.EXPECT().RemoveConsumer(ctx, gomock.Any()).Return(true).AnyTimes()
	store.EXPECT().GetConsumer(gomock.AssignableToTypeOf(subCtx), gomock.Any()).Return(consumer, nil)
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

	subs := &subscription.Model{
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
	subscriberID := "subscriber-id"

	mockBrokerStore.EXPECT().GetConsumer(gomock.Any(), gomock.Any()).Return(mockConsumer, nil)
	mockConsumer.EXPECT().Resume(gomock.AssignableToTypeOf(ctx), gomock.Any()).Return(nil).AnyTimes()

	// initialize delay consumer
	dc, err := NewDelayConsumer(ctx, subscriberID, "t1", subs, mockBrokerStore, mockHandler, cache, make(chan error))
	assert.NotNil(t, dc)
	assert.Nil(t, err)
	assert.NotNil(t, dc.LogFields())

	msgs := make([]messagebroker.ReceivedMessage, 0)
	msgs = append(msgs, getDummyBrokerMessage("msg-1", uuid.New().String(), time.Now().Add(-time.Second*100), 5))
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
	// message with delay within duration of test-case
	msg1 := getDummyBrokerMessage("msg-1", uuid.New().String(), time.Now().Add(time.Nanosecond*1000), 5)
	// message with delay outside duration of test-case
	msg2 := getDummyBrokerMessage("msg-2", uuid.New().String(), time.Now().Add(time.Second*2), 5)

	type Test struct {
		name                 string
		messages             []messagebroker.ReceivedMessage
		expectedMessage      []messagebroker.ReceivedMessage
		expectedMessageCount int
	}

	tests := []Test{
		{
			name:                 "DelayConsumer with message delivery time within test duration",
			messages:             []messagebroker.ReceivedMessage{msg1},
			expectedMessage:      []messagebroker.ReceivedMessage{},
			expectedMessageCount: 0,
		},
		{
			name:                 "DelayConsumer with message delivery time above test duration",
			messages:             []messagebroker.ReceivedMessage{msg2},
			expectedMessage:      []messagebroker.ReceivedMessage{msg2},
			expectedMessageCount: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// since new context is needed to for each time so set up is called each time
			ctx, ctrl, subCtx, cancel, brokerStore, handler, consumer, cache := setupDC(t)
			subs := getMockSubscriptionModel()
			subscriberID := "subscriber-id"
			errCh := make(chan error)
			dc, err := NewDelayConsumer(subCtx, subscriberID, "t1", subs, brokerStore, handler, cache, errCh)
			assert.NotNil(t, dc)
			assert.Nil(t, err)
			assert.NotNil(t, dc.LogFields())

			producer := getMockProducer(ctx, ctrl)
			brokerStore.EXPECT().GetProducer(gomock.Any(), gomock.Any()).Return(producer, nil).AnyTimes()

			go dc.Run(ctx)

			messages := make([]messagebroker.ReceivedMessage, 0)
			messages = append(messages, test.messages...)
			count := 0
			consumer.EXPECT().ReceiveMessages(gomock.AssignableToTypeOf(subCtx), gomock.Any()).DoAndReturn(
				func(arg0 context.Context, arg1 messagebroker.GetMessagesFromTopicRequest) (*messagebroker.GetMessagesFromTopicResponse, error) {
					if count == 0 {
						count++
						return &messagebroker.GetMessagesFromTopicResponse{Messages: messages}, nil
					} else {
						return &messagebroker.GetMessagesFromTopicResponse{}, nil
					}
				}).AnyTimes()

			select {
			case err := <-errCh:
				assert.Fail(t, err.Error())
			case <-time.NewTicker(time.Millisecond * 500).C:
				assert.Equal(t, len(dc.cachedMsgs), test.expectedMessageCount)
				assert.True(t, reflect.DeepEqual(dc.cachedMsgs, test.expectedMessage))
			}

			cancel()
			<-dc.doneCh
		})
	}
}

func TestDelayConsumer_Run_DeadLetter(t *testing.T) {
	ctx, ctrl, subCtx, cancel, brokerStore, handler, consumer, cache := setupDC(t)
	subs := getMockSubscriptionModel()
	subscriberID := "subscriber-dl-id"
	errCh := make(chan error)
	dlqTopicChan := make(chan *messagebroker.SendMessageToTopicRequest)
	producer := getMockDLQProducer(ctx, ctrl, dlqTopicChan)
	brokerStore.EXPECT().GetProducer(gomock.AssignableToTypeOf(subCtx), messagebroker.ProducerClientOptions{
		Topic:     "dlt1",
		TimeoutMs: 100,
	}).Return(producer, nil).AnyTimes()
	dc, err := NewDelayConsumer(subCtx, subscriberID, "t1-dl", subs, brokerStore, handler, cache, errCh)

	assert.NotNil(t, dc)
	assert.Nil(t, err)
	assert.NotNil(t, dc.LogFields())

	messages := make([]messagebroker.ReceivedMessage, 0)
	msg := getDummyBrokerMessage("msg-2", uuid.New().String(), time.Now().Add(time.Second*2), 1)
	messages = append(messages, msg)
	count := 0

	consumer.EXPECT().ReceiveMessages(gomock.AssignableToTypeOf(subCtx), messagebroker.GetMessagesFromTopicRequest{
		NumOfMessages: 10,
		TimeoutMs:     100,
	}).DoAndReturn(
		func(arg0 context.Context, arg1 messagebroker.GetMessagesFromTopicRequest) (*messagebroker.GetMessagesFromTopicResponse, error) {
			if count == 0 {
				count++
				return &messagebroker.GetMessagesFromTopicResponse{Messages: messages}, nil
			} else {
				return &messagebroker.GetMessagesFromTopicResponse{}, nil
			}
		}).AnyTimes()

	expectedReq := getMockSendMessageToTopicRequest()

	go dc.Run(ctx)

	select {
	case err := <-errCh:
		assert.Fail(t, err.Error())
	case req := <-dlqTopicChan:
		assert.Equal(t, req, expectedReq)
	case <-time.NewTicker(time.Millisecond * 500).C:
		assert.FailNow(t, "test timed out")
	}
	cancel()
	<-dc.doneCh
}

func getDummyBrokerMessage(data, messageId string, nextDeliveryTime time.Time, maxRetryCount int32) messagebroker.ReceivedMessage {
	return messagebroker.ReceivedMessage{
		Data:       bytes.NewBufferString("abc").Bytes(),
		Topic:      "t1",
		Partition:  10,
		Offset:     234,
		Attributes: nil,
		MessageHeader: messagebroker.MessageHeader{
			MessageID:            messageId,
			PublishTime:          time.Now(),
			SourceTopic:          "st1",
			RetryTopic:           "rt1",
			Subscription:         "s1",
			CurrentRetryCount:    2,
			MaxRetryCount:        maxRetryCount,
			CurrentTopic:         "ct1",
			InitialDelayInterval: 10,
			CurrentDelayInterval: 90,
			ClosestDelayInterval: 150,
			DeadLetterTopic:      "dlt1",
			NextDeliveryTime:     nextDeliveryTime,
		},
	}
}

func getMockProducer(ctx context.Context, ctrl *gomock.Controller) *mocks2.MockProducer {
	producer := mocks2.NewMockProducer(ctrl)
	producer.EXPECT().SendMessage(gomock.Any(), gomock.Any()).Return(&messagebroker.SendMessageToTopicResponse{MessageID: uuid.New().String()}, nil).AnyTimes()
	return producer
}

func getMockDLQProducer(ctx context.Context, ctrl *gomock.Controller, dlqTopicChan chan *messagebroker.SendMessageToTopicRequest) *mocks2.MockProducer {
	producer := mocks2.NewMockProducer(ctrl)
	producer.EXPECT().SendMessage(gomock.Any(), gomock.Any()).DoAndReturn(
		func(arg0 context.Context, arg1 messagebroker.SendMessageToTopicRequest) (*messagebroker.SendMessageToTopicResponse, error) {
			dlqTopicChan <- getMockSendMessageToTopicRequest()
			return &messagebroker.SendMessageToTopicResponse{MessageID: uuid.New().String()}, nil
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
