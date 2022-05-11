package subscriber

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	mockBS "github.com/razorpay/metro/internal/brokerstore/mocks"
	mocks "github.com/razorpay/metro/internal/node/mocks/repo"
	"github.com/razorpay/metro/internal/offset"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/messagebroker"
	mockMB "github.com/razorpay/metro/pkg/messagebroker/mocks"
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

func TestSubscriber_PullMessages(t *testing.T) {
	ctrl := gomock.NewController(t)
	bs := mockBS.NewMockIBrokerStore(ctrl)
	cs := mockMB.NewMockConsumer(ctrl)
	ctx := context.Background()

	subID := "subscriber-id"
	subName := "subscription-name"
	topic := "primary-topic"
	retryTopic := "retry-topic"
	partition := int32(0)

	bs.EXPECT().GetConsumer(
		ctx,
		messagebroker.ConsumerClientOptions{
			Topics:          []string{topic, retryTopic},
			GroupID:         subName,
			GroupInstanceID: subID,
		},
	).Return(cs, nil)
	consumer, _ := NewConsumerManager(ctx, bs, 1000, subID, subName, topic, retryTopic)
	subImpl := &BasicImplementation{
		maxOutstandingMessages: 2,
		maxOutstandingBytes:    200,
		topic:                  topic,
		ctx:                    ctx,
		consumedMessageStats:   make(map[TopicPartition]*ConsumptionMetadata),
		subscriberID:           subID,
		subscription: &subscription.Model{
			Name:  subName,
			Topic: topic,
		},
	}

	req := messagebroker.GetTopicMetadataRequest{
		Topic:     topic,
		Partition: partition,
	}
	cs.EXPECT().GetTopicMetadata(ctx, req).Return(messagebroker.GetTopicMetadataResponse{}, nil)
	cs.EXPECT().Pause(ctx, gomock.Any()).Times(2).Return(nil)

	tests := []struct {
		maxNumOfMessages int32
		expected         []string
		err              error
		wantErr          bool
		consumer         IConsumer
	}{
		{
			maxNumOfMessages: 1,
			expected:         []string{},
			err:              nil,
			wantErr:          false,
			consumer:         nil,
		},
		{
			maxNumOfMessages: 3,
			expected:         []string{"a", "b", "c"},
			err:              nil,
			wantErr:          false,
			consumer:         consumer,
		},
		{
			maxNumOfMessages: 1,
			expected:         []string{},
			err:              fmt.Errorf("Consumer is paused"),
			wantErr:          true,
			consumer:         consumer,
		},
	}

	for _, test := range tests {
		subImpl.consumer = test.consumer
		messages := make([]messagebroker.ReceivedMessage, 0, test.maxNumOfMessages)
		for index, msg := range test.expected {
			pubSub := &metrov1.PubsubMessage{Data: []byte(msg)}
			data, _ := proto.Marshal(pubSub)
			msgProto := messagebroker.ReceivedMessage{
				Data:      data,
				Topic:     topic,
				Partition: partition,
			}
			msgProto.MessageID = strconv.Itoa(index)
			messages = append(messages, msgProto)
		}

		if subImpl.consumer != nil {
			cs.EXPECT().ReceiveMessages(ctx, messagebroker.GetMessagesFromTopicRequest{NumOfMessages: test.maxNumOfMessages, TimeoutMs: 1000}).Return(
				&messagebroker.GetMessagesFromTopicResponse{
					Messages: messages,
				}, test.err,
			)
		}

		pullRequest := &PullRequest{ctx: ctx, MaxNumOfMessages: test.maxNumOfMessages}
		responseChan := make(chan *metrov1.PullResponse, 10)
		errChan := make(chan error, 10)
		subImpl.Pull(ctx, pullRequest, responseChan, errChan)

		ticker := time.NewTimer(30 * time.Second)

		select {
		case resp := <-responseChan:
			assert.Equal(t, len(test.expected), len(resp.ReceivedMessages))
			for index, msg := range resp.ReceivedMessages {
				assert.Equal(t, test.expected[index], string(msg.Message.Data))
			}
		case err := <-errChan:
			assert.Equal(t, test.wantErr, err != nil)
		case <-ticker.C:
			assert.FailNow(t, "Test case timed out")
		default:
			assert.Zero(t, len(test.expected))
			assert.False(t, test.wantErr)
		}
	}
}

func TestSubscriber_AcknowledgelMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	bs := mockBS.NewMockIBrokerStore(ctrl)
	cs := mockMB.NewMockConsumer(ctrl)
	ctx := context.Background()
	offsetRepo := mocks.NewMockIRepo(ctrl)
	offsetCore := offset.NewCore(offsetRepo)

	subID := "subscriber-id"
	subName := "subscription-name"
	topic := "primary-topic"
	retryTopic := "retry-topic"
	partition := int32(0)

	offsetRepo.EXPECT().Exists(gomock.Any(), gomock.Any()).AnyTimes()
	offsetRepo.EXPECT().Save(gomock.Any(), gomock.Any()).AnyTimes()

	bs.EXPECT().GetConsumer(
		ctx,
		messagebroker.ConsumerClientOptions{
			Topics:          []string{topic, retryTopic},
			GroupID:         subName,
			GroupInstanceID: subID,
		},
	).Return(cs, nil)
	consumer, _ := NewConsumerManager(ctx, bs, 1000, subID, subName, topic, retryTopic)
	subImpl := &BasicImplementation{
		maxOutstandingMessages: 1,
		topic:                  topic,
		ctx:                    ctx,
		consumedMessageStats:   make(map[TopicPartition]*ConsumptionMetadata),
		subscriberID:           subID,
		consumer:               consumer,
		subscription: &subscription.Model{
			Name:  subName,
			Topic: topic,
		},
		offsetCore: offsetCore,
	}

	req := messagebroker.GetTopicMetadataRequest{
		Topic:     topic,
		Partition: partition,
	}
	cs.EXPECT().GetTopicMetadata(ctx, req).Return(messagebroker.GetTopicMetadataResponse{Offset: 0}, nil)
	cs.EXPECT().Pause(ctx, gomock.Any()).AnyTimes().Return(nil)

	testInputs := []struct {
		message            string
		offset             int32
		messageId          string
		ackId              string
		canConsumeMore     bool
		maxCommittedOffset int32
	}{
		{
			message:            "there",
			offset:             2,
			messageId:          "1",
			canConsumeMore:     false,
			maxCommittedOffset: 0,
		},
		{
			message:            "Hello",
			offset:             1,
			messageId:          "2",
			canConsumeMore:     true,
			maxCommittedOffset: 2,
		},
		{
			message:            "Razor",
			offset:             3,
			messageId:          "3",
			canConsumeMore:     true,
			maxCommittedOffset: 3,
		},
	}

	messages := make([]messagebroker.ReceivedMessage, 0, 3)
	for _, input := range testInputs {
		data, _ := proto.Marshal(&metrov1.PubsubMessage{Data: []byte(input.message)})
		msgProto := messagebroker.ReceivedMessage{
			Data:      data,
			Topic:     topic,
			Partition: partition,
			Offset:    input.offset,
		}
		msgProto.MessageID = input.messageId
		messages = append(messages, msgProto)
	}
	cs.EXPECT().ReceiveMessages(ctx, gomock.Any()).Return(
		&messagebroker.GetMessagesFromTopicResponse{
			Messages: messages,
		}, nil,
	)
	cs.EXPECT().CommitByPartitionAndOffset(ctx, gomock.Any()).Return(
		messagebroker.CommitOnTopicResponse{}, nil,
	).AnyTimes()

	pullRequest := &PullRequest{ctx: ctx, MaxNumOfMessages: int32(len(testInputs))}
	responseChan := make(chan *metrov1.PullResponse, 1)
	errChan := make(chan error, 10)
	subImpl.Pull(ctx, pullRequest, responseChan, errChan)

	ticker := time.NewTimer(30 * time.Second)
	select {
	case resp := <-responseChan:
		tp := NewTopicPartition(topic, partition)
		assert.Equal(t, len(testInputs), len(resp.ReceivedMessages))
		for index, msg := range resp.ReceivedMessages {
			testInputs[index].ackId = msg.AckId
		}

		for _, test := range testInputs {
			ackMsg, _ := ParseAckID(test.ackId)
			subImpl.Acknowledge(ctx, ackMsg, errChan)
			assert.Equal(t, test.canConsumeMore, subImpl.CanConsumeMore())
			assert.Equal(t, test.maxCommittedOffset, subImpl.consumedMessageStats[tp].maxCommittedOffset)
		}
	case <-ticker.C:
		assert.FailNow(t, "Test case timed out")
	}
}

func TestSubscriber_ModAckAndEvictPastDeadline(t *testing.T) {
	ctrl := gomock.NewController(t)
	bs := mockBS.NewMockIBrokerStore(ctrl)
	cs := mockMB.NewMockConsumer(ctrl)
	ctx := context.Background()
	offsetRepo := mocks.NewMockIRepo(ctrl)
	offsetCore := offset.NewCore(offsetRepo)

	subID := "subscriber-id"
	subName := "subscription-name"
	topic := "primary-topic"
	retryTopic := "retry-topic"
	partition := int32(0)

	offsetRepo.EXPECT().Exists(gomock.Any(), gomock.Any()).AnyTimes()
	offsetRepo.EXPECT().Save(gomock.Any(), gomock.Any()).AnyTimes()

	bs.EXPECT().GetConsumer(
		ctx,
		messagebroker.ConsumerClientOptions{
			Topics:          []string{topic, retryTopic},
			GroupID:         subName,
			GroupInstanceID: subID,
		},
	).Return(cs, nil)
	consumer, _ := NewConsumerManager(ctx, bs, 1000, subID, subName, topic, retryTopic)
	subImpl := &BasicImplementation{
		maxOutstandingMessages: 1,
		topic:                  topic,
		ctx:                    ctx,
		consumedMessageStats:   make(map[TopicPartition]*ConsumptionMetadata),
		subscriberID:           subID,
		consumer:               consumer,
		subscription: &subscription.Model{
			Name:  subName,
			Topic: topic,
		},
		offsetCore: offsetCore,
	}

	req := messagebroker.GetTopicMetadataRequest{
		Topic:     topic,
		Partition: partition,
	}
	cs.EXPECT().GetTopicMetadata(ctx, req).Return(messagebroker.GetTopicMetadataResponse{Offset: 0}, nil)
	cs.EXPECT().Pause(ctx, gomock.Any()).AnyTimes().Return(nil)

	testInputs := []struct {
		message              string
		messageId            string
		ackId                string
		ackDeadline          int32
		consumedMessageCount int
	}{
		{
			message:              "Hello",
			messageId:            "1",
			ackDeadline:          int32(0),
			consumedMessageCount: 1,
		},
		{
			message:              "there",
			messageId:            "2",
			ackDeadline:          int32(time.Now().Add(1 * time.Millisecond).Unix()),
			consumedMessageCount: 1,
		},
	}

	messages := make([]messagebroker.ReceivedMessage, 0, 3)
	for _, input := range testInputs {
		data, _ := proto.Marshal(&metrov1.PubsubMessage{Data: []byte(input.message)})
		msgProto := messagebroker.ReceivedMessage{
			Data:      data,
			Topic:     topic,
			Partition: partition,
		}
		msgProto.MessageID = input.messageId
		messages = append(messages, msgProto)
	}

	cs.EXPECT().ReceiveMessages(ctx, gomock.Any()).Return(
		&messagebroker.GetMessagesFromTopicResponse{
			Messages: messages,
		}, nil,
	)
	cs.EXPECT().CommitByPartitionAndOffset(ctx, gomock.Any()).Return(
		messagebroker.CommitOnTopicResponse{}, nil,
	).AnyTimes()

	pullRequest := &PullRequest{ctx: ctx, MaxNumOfMessages: 1}
	responseChan := make(chan *metrov1.PullResponse, 1)
	errChan := make(chan error, 10)
	subImpl.Pull(ctx, pullRequest, responseChan, errChan)

	ticker := time.NewTimer(30 * time.Second)
	select {
	case resp := <-responseChan:
		assert.NotEmpty(t, resp.ReceivedMessages)

		tp := NewTopicPartition(topic, partition)
		assert.Equal(t, len(testInputs), len(resp.ReceivedMessages))
		for index, msg := range resp.ReceivedMessages {
			testInputs[index].ackId = msg.AckId
		}

		for _, test := range testInputs {
			ackMsg, _ := ParseAckID(test.ackId)
			modAckReq := &ModAckMessage{
				ctx:         ctx,
				AckMessage:  ackMsg,
				ackDeadline: test.ackDeadline,
			}
			subImpl.ModAckDeadline(ctx, modAckReq, errChan)
			assert.Equal(t, test.consumedMessageCount, len(subImpl.consumedMessageStats[tp].consumedMessages))
		}

		time.Sleep(1 * time.Second)
		subImpl.EvictUnackedMessagesPastDeadline(ctx, errChan)
		assert.Equal(t, 0, len(subImpl.consumedMessageStats[tp].consumedMessages))
	case <-ticker.C:
		assert.FailNow(t, "Test case timed out")
	}
}
