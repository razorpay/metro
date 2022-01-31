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
	dc, err := NewDelayConsumer(ctx, subscriberID, "t1", 0, subs, mockBrokerStore, mockHandler)
	assert.NotNil(t, dc)
	assert.Nil(t, err)
	assert.NotNil(t, dc.LogFields())

	msgs := make([]messagebroker.ReceivedMessage, 0)
	msgs = append(msgs, getDummyBrokerMessage())
	resp := &messagebroker.GetMessagesFromTopicResponse{Messages: msgs}
	mockConsumer.EXPECT().ReceiveMessages(gomock.Any(), gomock.Any()).Return(resp, nil).AnyTimes()

	// on new message from broker
	mockHandler.EXPECT().Do(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockConsumer.EXPECT().CommitByPartitionAndOffset(gomock.Any(), gomock.Any()).Return(messagebroker.CommitOnTopicResponse{}, nil).AnyTimes()

	// on context cancellation
	mockBrokerStore.EXPECT().RemoveConsumer(gomock.Any(), gomock.Any()).Return(true)
	mockConsumer.EXPECT().Close(gomock.Any()).Return(nil)

	// running the consumer with a deterministic timeout
	dc.Run(ctx)
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
