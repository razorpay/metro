// +build unit

package subscriber

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/magiconair/properties/assert"
	offsetMocks "github.com/razorpay/metro/internal/offset/mocks/core"
	"github.com/razorpay/metro/internal/subscriber/customheap"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/messagebroker"
	messageBrokerMocks "github.com/razorpay/metro/pkg/messagebroker/mocks"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSubscriber_Pull_Filtering(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	subscription := &subscription.Model{
		Name:  "test_sub",
		Topic: "test_topic",
	}

	messages := []*metrov1.PublishRequest{
		{
			Topic: "test_topic",
			Messages: []*metrov1.PubsubMessage{
				{
					Data: bytes.NewBufferString("abc").Bytes(),
					Attributes: map[string]string{
						"domain": "com",
					},
					MessageId:   "msg_1",
					PublishTime: timestamppb.Now(),
				},
			},
		},
		{
			Topic: "test_topic",
			Messages: []*metrov1.PubsubMessage{
				{
					Data: bytes.NewBufferString("abc").Bytes(),
					Attributes: map[string]string{
						"domain": "com",
						"x":      "org",
					},
					MessageId:   "msg_1",
					PublishTime: timestamppb.Now(),
				},
			},
		},
		{
			Topic: "test_topic",
			Messages: []*metrov1.PubsubMessage{
				{
					Data: bytes.NewBufferString("abc").Bytes(),
					Attributes: map[string]string{
						"domain": "org",
					},
					MessageId:   "msg_1",
					PublishTime: timestamppb.Now(),
				},
			},
		},
		{
			Topic: "test_topic",
			Messages: []*metrov1.PubsubMessage{
				{
					Data:        bytes.NewBufferString("abc").Bytes(),
					MessageId:   "msg_1",
					PublishTime: timestamppb.Now(),
				},
			},
		},
		{
			Topic: "test_topic",
			Messages: []*metrov1.PubsubMessage{
				{
					Data: bytes.NewBufferString("abc").Bytes(),
					Attributes: map[string]string{
						"x": "company",
					},
					MessageId:   "msg_1",
					PublishTime: timestamppb.Now(),
				},
			},
		},
		{
			Topic: "test_topic",
			Messages: []*metrov1.PubsubMessage{
				{
					Data: bytes.NewBufferString("abc").Bytes(),
					Attributes: map[string]string{
						"x": "dotcom",
					},
					MessageId:   "msg_1",
					PublishTime: timestamppb.Now(),
				},
			},
		},
		{
			Topic: "test_topic",
			Messages: []*metrov1.PubsubMessage{
				{
					Data: bytes.NewBufferString("abc").Bytes(),
					Attributes: map[string]string{
						"x": "razor",
					},
					MessageId:   "msg_1",
					PublishTime: timestamppb.Now(),
				},
			},
		},
	}

	tests := []struct {
		FilterExpression  string
		expectedNumOfMsgs int
		numOfCommits      int
	}{
		{
			FilterExpression:  "attributes:domain",
			expectedNumOfMsgs: 3,
			numOfCommits:      4,
		},
		{
			FilterExpression:  "attributes:domain AND attributes.x = \"org\"",
			expectedNumOfMsgs: 1,
			numOfCommits:      6,
		},
		{
			FilterExpression:  "hasPrefix(attributes.domain, \"co\") OR hasPrefix(attributes.x, \"co\")",
			expectedNumOfMsgs: 3,
			numOfCommits:      4,
		},
		{
			FilterExpression:  "(attributes:domain AND attributes.domain = \"com\") OR (attributes:x AND NOT hasPrefix(attributes.x,\"co\"))",
			expectedNumOfMsgs: 4,
			numOfCommits:      3,
		},
	}

	receivedMsgs := make(map[string]messagebroker.ReceivedMessage)

	for idx, msg := range messages {
		key := fmt.Sprintf("msg_%v", idx)
		m, _ := proto.Marshal(msg.Messages[0])
		receivedMsgs[key] = messagebroker.ReceivedMessage{
			Data:      m,
			Topic:     "test_topic",
			Partition: 1,
		}
	}
	brokerResp := &messagebroker.GetMessagesFromTopicResponse{
		PartitionOffsetWithMessages: receivedMsgs,
	}

	for _, test := range tests {
		respChan := make(chan *metrov1.PullResponse)
		mockConsumer := messageBrokerMocks.NewMockConsumer(ctrl)
		mockOffsetCore := offsetMocks.NewMockICore(ctrl)
		mockConsumer.EXPECT().ReceiveMessages(gomock.Any(), gomock.Any()).Return(brokerResp, nil)
		mockOffsetCore.EXPECT().SetOffset(gomock.Any(), gomock.Any()).Return(nil).Times(int(test.numOfCommits))
		mockConsumer.EXPECT().CommitByPartitionAndOffset(gomock.Any(), gomock.Any()).
			Return(messagebroker.CommitOnTopicResponse{}, nil).Times(int(test.numOfCommits))
		consumedMsgStats := map[TopicPartition]*ConsumptionMetadata{
			NewTopicPartition("test_topic", 1): &ConsumptionMetadata{
				consumedMessages:     make(map[string]interface{}),
				offsetBasedMinHeap:   customheap.NewOffsetBasedPriorityQueue(),
				deadlineBasedMinHeap: customheap.NewDeadlineBasedPriorityQueue(),
			},
		}
		s := &Subscriber{
			subscription:         subscription,
			subscriberID:         "test_id",
			responseChan:         respChan,
			consumer:             mockConsumer,
			ctx:                  ctx,
			consumedMessageStats: consumedMsgStats,
			offsetCore:           mockOffsetCore,
		}
		s.subscription.SetFilterExpression(test.FilterExpression)
		request := &PullRequest{
			ctx:              ctx,
			MaxNumOfMessages: 10,
		}
		go s.pull(request)
		select {
		case data := <-respChan:
			assert.Equal(t, len(data.ReceivedMessages), test.expectedNumOfMsgs)
		case <-time.After(2 * time.Second):
			fmt.Println("Timeout")
		}
	}
}
