package publisher

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	brokerstoremock "github.com/razorpay/metro/internal/brokerstore/mocks"
	"github.com/razorpay/metro/pkg/messagebroker"
	mockMB "github.com/razorpay/metro/pkg/messagebroker/mocks"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/rs/xid"
	"github.com/stretchr/testify/assert"
)

func TestCore_Publish(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := context.Background()
	mockBrokerStore := brokerstoremock.NewMockIBrokerStore(ctrl)
	producer := mockMB.NewMockProducer(ctrl)
	publisherCore := NewCore(mockBrokerStore)
	req := getDummyPublishRequest()
	msgID := xid.New().String()

	tests := []struct {
		name     string
		req      *metrov1.PublishRequest
		msgID    string
		expected []string
		wantErr  bool
		err      error
	}{
		{
			name:     "Message publishing with no errors",
			req:      req,
			msgID:    msgID,
			expected: []string{msgID},
			wantErr:  false,
			err:      nil,
		},
		{
			name:     "Message publishing with errors",
			req:      req,
			msgID:    "",
			expected: nil,
			wantErr:  true,
			err:      fmt.Errorf("Something went wrong"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockBrokerStore.EXPECT().GetProducer(
				gomock.Any(),
				messagebroker.ProducerClientOptions{Topic: test.req.Topic, TimeoutMs: 500},
			).Return(producer, nil).AnyTimes()
			producer.EXPECT().SendMessage(gomock.Any(), gomock.Any()).DoAndReturn(
				func(
					ctx context.Context,
					req messagebroker.SendMessageToTopicRequest,
				) (*messagebroker.SendMessageToTopicResponse, error) {
					return &messagebroker.SendMessageToTopicResponse{MessageID: test.msgID}, test.err
				}).AnyTimes()
			msgIds, err := publisherCore.Publish(ctx, test.req)
			assert.Equal(t, test.wantErr, err != nil)
			assert.Equal(t, test.expected, msgIds)
		})
	}
}

func getDummyPublishRequest() *metrov1.PublishRequest {
	return &metrov1.PublishRequest{
		Topic: "projects/project-001/topics/topic-001",
		Messages: []*metrov1.PubsubMessage{
			{
				Data:        []byte("d1"),
				OrderingKey: "o1",
			},
		},
	}
}
