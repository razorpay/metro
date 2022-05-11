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

const (
	SubID      = "subscriber-id"
	SubName    = "subscription-name"
	Topic      = "primary-topic"
	RetryTopic = "retry-topic"
	Partition  = int32(0)
)

func TestBasicImplementation_Pull(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	cs, consumer := getMockConsumerAndManager(t, ctx, ctrl)
	subImpl := getBasicImplementation(ctx, consumer, nil)

	tests := []struct {
		maxNumOfMessages int32
		expected         []string
		err              error
		wantErr          bool
		consumer         IConsumer
	}{
		{
			maxNumOfMessages: 2,
			expected:         []string{"a", "b"},
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
		messages := make([]messagebroker.ReceivedMessage, 0, test.maxNumOfMessages)
		for index, msg := range test.expected {
			pubSub := &metrov1.PubsubMessage{Data: []byte(msg)}
			data, _ := proto.Marshal(pubSub)
			msgProto := messagebroker.ReceivedMessage{
				Data:      data,
				Topic:     Topic,
				Partition: Partition,
			}
			msgProto.MessageID = strconv.Itoa(index)
			messages = append(messages, msgProto)
		}

		cs.EXPECT().ReceiveMessages(ctx, messagebroker.GetMessagesFromTopicRequest{NumOfMessages: test.maxNumOfMessages, TimeoutMs: 1000}).Return(
			&messagebroker.GetMessagesFromTopicResponse{
				Messages: messages,
			}, test.err,
		)

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

func TestBasicImplementation_Acknowledge(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := context.Background()
	offsetRepo := mocks.NewMockIRepo(ctrl)
	offsetCore := offset.NewCore(offsetRepo)
	offsetRepo.EXPECT().Exists(gomock.Any(), gomock.Any()).AnyTimes()
	offsetRepo.EXPECT().Save(gomock.Any(), gomock.Any()).AnyTimes()

	cs, consumer := getMockConsumerAndManager(t, ctx, ctrl)
	subImpl := getBasicImplementation(ctx, consumer, offsetCore)

	testInputs := []struct {
		message            string
		offset             int32
		messageID          string
		canConsumeMore     bool
		maxCommittedOffset int32
	}{
		{
			message:            "there",
			offset:             2,
			messageID:          "1",
			canConsumeMore:     false,
			maxCommittedOffset: 0,
		},
		{
			message:            "Hello",
			offset:             1,
			messageID:          "2",
			canConsumeMore:     true,
			maxCommittedOffset: 2,
		},
		{
			message:            "Razor",
			offset:             3,
			messageID:          "3",
			canConsumeMore:     true,
			maxCommittedOffset: 3,
		},
	}

	messages := make([]messagebroker.ReceivedMessage, 0, 3)
	for _, input := range testInputs {
		data, _ := proto.Marshal(&metrov1.PubsubMessage{Data: []byte(input.message)})
		msgProto := messagebroker.ReceivedMessage{
			Data:      data,
			Topic:     Topic,
			Partition: Partition,
			Offset:    input.offset,
		}
		msgProto.MessageID = input.messageID
		messages = append(messages, msgProto)
	}
	cs.EXPECT().ReceiveMessages(ctx, gomock.Any()).Return(
		&messagebroker.GetMessagesFromTopicResponse{
			Messages: messages,
		}, nil,
	)

	pullRequest := &PullRequest{ctx: ctx, MaxNumOfMessages: int32(len(testInputs))}
	responseChan := make(chan *metrov1.PullResponse, 1)
	errChan := make(chan error, 10)
	subImpl.Pull(ctx, pullRequest, responseChan, errChan)

	ticker := time.NewTimer(30 * time.Second)
	select {
	case resp := <-responseChan:
		tp := NewTopicPartition(Topic, Partition)
		assert.Equal(t, len(testInputs), len(resp.ReceivedMessages))
		for index, msg := range resp.ReceivedMessages {
			ackMsg, _ := ParseAckID(msg.AckId)
			subImpl.Acknowledge(ctx, ackMsg, errChan)
			assert.Equal(t, testInputs[index].canConsumeMore, subImpl.CanConsumeMore())
			assert.Equal(t, testInputs[index].maxCommittedOffset, subImpl.consumedMessageStats[tp].maxCommittedOffset)
		}
	case <-ticker.C:
		assert.FailNow(t, "Test case timed out")
	}
}

func TestBasicImplementation_ModAckDeadline(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := context.Background()
	offsetRepo := mocks.NewMockIRepo(ctrl)
	offsetCore := offset.NewCore(offsetRepo)

	offsetRepo.EXPECT().Exists(gomock.Any(), gomock.Any()).AnyTimes()
	offsetRepo.EXPECT().Save(gomock.Any(), gomock.Any()).AnyTimes()

	cs, consumer := getMockConsumerAndManager(t, ctx, ctrl)
	subImpl := getBasicImplementation(ctx, consumer, offsetCore)

	testInputs := []struct {
		message              string
		messageID            string
		ackDeadline          int32
		consumedMessageCount int
	}{
		{
			message:              "Hello",
			messageID:            "1",
			ackDeadline:          int32(0),
			consumedMessageCount: 1,
		},
		{
			message:              "there",
			messageID:            "2",
			ackDeadline:          int32(time.Now().Add(1 * time.Nanosecond).Unix()),
			consumedMessageCount: 1,
		},
	}

	messages := make([]messagebroker.ReceivedMessage, 0, 3)
	for _, input := range testInputs {
		data, _ := proto.Marshal(&metrov1.PubsubMessage{Data: []byte(input.message)})
		msgProto := messagebroker.ReceivedMessage{
			Data:      data,
			Topic:     Topic,
			Partition: Partition,
		}
		msgProto.MessageID = input.messageID
		messages = append(messages, msgProto)
	}

	cs.EXPECT().ReceiveMessages(ctx, gomock.Any()).Return(
		&messagebroker.GetMessagesFromTopicResponse{
			Messages: messages,
		}, nil,
	)

	pullRequest := &PullRequest{ctx: ctx, MaxNumOfMessages: 1}
	responseChan := make(chan *metrov1.PullResponse, 1)
	errChan := make(chan error, 10)
	subImpl.Pull(ctx, pullRequest, responseChan, errChan)

	ticker := time.NewTimer(30 * time.Second)
	select {
	case resp := <-responseChan:
		assert.NotEmpty(t, resp.ReceivedMessages)

		tp := NewTopicPartition(Topic, Partition)
		assert.Equal(t, len(testInputs), len(resp.ReceivedMessages))
		for index, msg := range resp.ReceivedMessages {
			ackMsg, _ := ParseAckID(msg.AckId)
			modAckReq := &ModAckMessage{
				ctx:         ctx,
				AckMessage:  ackMsg,
				ackDeadline: testInputs[index].ackDeadline,
			}
			subImpl.ModAckDeadline(ctx, modAckReq, errChan)
			assert.Equal(t, testInputs[index].consumedMessageCount, len(subImpl.consumedMessageStats[tp].consumedMessages))
		}

		ticker := time.NewTimer(1 * time.Second)
		select {
		case <-ticker.C:
			subImpl.EvictUnackedMessagesPastDeadline(ctx, errChan)
			assert.Equal(t, 0, len(subImpl.consumedMessageStats[tp].consumedMessages))
		}
	case <-ticker.C:
		assert.FailNow(t, "Test case timed out")
	}
}

func getBasicImplementation(ctx context.Context, consumer IConsumer, offsetCore offset.ICore) *BasicImplementation {
	return &BasicImplementation{
		maxOutstandingMessages: 1,
		maxOutstandingBytes:    100,
		topic:                  Topic,
		ctx:                    ctx,
		consumedMessageStats:   make(map[TopicPartition]*ConsumptionMetadata),
		subscriberID:           SubID,
		consumer:               consumer,
		subscription: &subscription.Model{
			Name:  SubName,
			Topic: Topic,
		},
		offsetCore: offsetCore,
	}
}

func getMockConsumerAndManager(
	t *testing.T,
	ctx context.Context,
	ctrl *gomock.Controller,
) (*mockMB.MockConsumer, IConsumer) {
	cs := mockMB.NewMockConsumer(ctrl)
	bs := mockBS.NewMockIBrokerStore(ctrl)
	bs.EXPECT().GetConsumer(
		ctx,
		messagebroker.ConsumerClientOptions{
			Topics:          []string{Topic, RetryTopic},
			GroupID:         SubName,
			GroupInstanceID: SubID,
		},
	).Return(cs, nil)
	consumer, _ := NewConsumerManager(ctx, bs, 1000, SubID, SubName, Topic, RetryTopic)
	req := messagebroker.GetTopicMetadataRequest{
		Topic:     Topic,
		Partition: Partition,
	}
	cs.EXPECT().GetTopicMetadata(ctx, req).Return(messagebroker.GetTopicMetadataResponse{}, nil)
	cs.EXPECT().Pause(ctx, gomock.Any()).AnyTimes().Return(nil)
	cs.EXPECT().CommitByPartitionAndOffset(ctx, gomock.Any()).Return(
		messagebroker.CommitOnTopicResponse{}, nil,
	).AnyTimes()
	return cs, consumer
}
