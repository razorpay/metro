package subscriber

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	mocks "github.com/razorpay/metro/internal/node/mocks/repo"
	"github.com/razorpay/metro/internal/offset"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/messagebroker"
	mockMB "github.com/razorpay/metro/pkg/messagebroker/mocks"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/stretchr/testify/assert"
)

func orderedSetup(t *testing.T) (
	ctx context.Context,
	cs *mockMB.MockConsumer,
	subImpl *OrderedImplementation,
) {
	ctrl := gomock.NewController(t)
	ctx = context.Background()
	cs = getMockConsumer(ctx, ctrl)
	consumer := getMockConsumerManager(ctx, ctrl, cs)
	subImpl = getMockOrderedImplementation(ctx, consumer, ctrl)
	return
}

func TestOrderedImplementation_Pull(t *testing.T) {
	ctx, cs, subImpl := orderedSetup(t)
	tests := []struct {
		maxNumOfMessages int32
		input            []string
		expected         []string
		err              error
		wantErr          bool
		pausedMessages   []string
	}{
		{
			maxNumOfMessages: 2,
			input:            []string{"a", "b"},
			expected:         []string{"c", "a", "b"},
			pausedMessages:   []string{"c"},
			err:              nil,
			wantErr:          false,
		},
		{
			maxNumOfMessages: 1,
			pausedMessages:   []string{},
			input:            []string{},
			expected:         []string{},
			err:              fmt.Errorf("Consumer is paused"),
			wantErr:          true,
		},
	}

	for _, test := range tests {
		subImpl.pausedMessages = getMockReceivedMessages(test.pausedMessages, "test-key")
		cs.EXPECT().ReceiveMessages(ctx, messagebroker.GetMessagesFromTopicRequest{NumOfMessages: test.maxNumOfMessages, TimeoutMs: 1000}).Return(
			&messagebroker.GetMessagesFromTopicResponse{
				Messages: getMockReceivedMessages(test.input, "test-key"),
			}, test.err,
		)

		pullRequest := &PullRequest{ctx: ctx, MaxNumOfMessages: test.maxNumOfMessages}
		responseChan := make(chan *metrov1.PullResponse, 10)
		errChan := make(chan error, 10)
		subImpl.Pull(ctx, pullRequest, responseChan, errChan)

		select {
		case resp := <-responseChan:
			assert.Equal(t, len(test.expected), len(resp.ReceivedMessages))
			got := func() []string {
				messages := make([]string, 0, len(resp.ReceivedMessages))
				for _, msg := range resp.ReceivedMessages {
					messages = append(messages, string(msg.Message.Data))
				}
				return messages
			}()
			if !reflect.DeepEqual(got, test.expected) {
				t.Errorf("Pull() = %v, want %v", got, test.expected)
			}
		case err := <-errChan:
			assert.Equal(t, test.wantErr, err != nil)
		case <-ticker.C:
			assert.FailNow(t, "Test case timed out")
		}
	}
}

func getMockOrderedImplementation(ctx context.Context, consumer IConsumer, ctrl *gomock.Controller) *OrderedImplementation {
	offsetRepo := mocks.NewMockIRepo(ctrl)
	offsetCore := offset.NewCore(offsetRepo)

	offsetRepo.EXPECT().Exists(gomock.Any(), gomock.Any()).AnyTimes()
	offsetRepo.EXPECT().Save(gomock.Any(), gomock.Any()).AnyTimes()

	return &OrderedImplementation{
		maxOutstandingMessages: 1,
		maxOutstandingBytes:    100,
		topic:                  topic,
		subscriberID:           subID,
		consumer:               consumer,
		offsetCore:             offsetCore,
		ctx:                    ctx,
		subscription: &subscription.Model{
			Name:  subName,
			Topic: topic,
		},
		consumedMessageStats: make(map[TopicPartition]*OrderedConsumptionMetadata),
		pausedMessages:       make([]messagebroker.ReceivedMessage, 0),
		sequenceManager:      NewOffsetSequenceManager(ctx, offsetCore),
	}
}

func TestOrderedImplementation_Acknowledge(t *testing.T) {
	ctx, cs, subImpl := orderedSetup(t)
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
			Data:        data,
			Topic:       topic,
			Partition:   partition,
			Offset:      input.offset,
			OrderingKey: "test-key",
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

	select {
	case resp := <-responseChan:
		tp := NewTopicPartition(topic, partition)
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
