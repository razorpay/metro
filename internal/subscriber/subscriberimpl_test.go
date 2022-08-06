package subscriber

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	mockBS "github.com/razorpay/metro/internal/brokerstore/mocks"
	mocks "github.com/razorpay/metro/internal/node/mocks/repo"
	"github.com/razorpay/metro/internal/offset"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/messagebroker"
	mockMB "github.com/razorpay/metro/pkg/messagebroker/mocks"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/stretchr/testify/assert"
)

var tickerTimeout = 2 * time.Second

const (
	subID       string = "subscriber-id"
	subName     string = "subscription-name"
	topicName   string = "primary-topic"
	retryTopic  string = "retry-topic"
	partition   int32  = 0
	uberTraceID string = "uber-trace_id"
)

func setup(t *testing.T) (
	ctx context.Context,
	cs *mockMB.MockConsumer,
	subImpl *BasicImplementation,
) {
	ctrl := gomock.NewController(t)
	ctx = context.Background()
	cs = getMockConsumer(ctx, ctrl)
	consumer := getMockConsumerManager(ctx, ctrl, cs)
	subImpl = getMockBasicImplementation(ctx, consumer, ctrl)
	return
}

func TestBasicImplementation_Pull(t *testing.T) {
	ctx, cs, subImpl := setup(t)
	tests := []struct {
		maxNumOfMessages int32
		expected         []string
		err              error
		wantErr          bool
	}{
		{
			maxNumOfMessages: 2,
			expected:         []string{"a", "b"},
			err:              nil,
			wantErr:          false,
		},
		{
			maxNumOfMessages: 1,
			expected:         []string{},
			err:              fmt.Errorf("Consumer is paused"),
			wantErr:          true,
		},
	}

	for _, test := range tests {
		cs.EXPECT().ReceiveMessages(ctx, messagebroker.GetMessagesFromTopicRequest{NumOfMessages: test.maxNumOfMessages, TimeoutMs: 1000}).Return(
			&messagebroker.GetMessagesFromTopicResponse{
				Messages: getMockReceivedMessages(test.expected),
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
		case <-time.NewTicker(tickerTimeout).C:
			assert.FailNow(t, "Test case timed out")
		}
	}
}

func TestBasicImplementation_Acknowledge(t *testing.T) {
	ctx, cs, subImpl := setup(t)
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
			Topic:     topicName,
			Partition: partition,
			Offset:    input.offset,
		}
		msgProto.MessageID = input.messageID
		messages = append(messages, msgProto)
	}

	cs.EXPECT().ReceiveMessages(ctx, messagebroker.GetMessagesFromTopicRequest{NumOfMessages: int32(len(testInputs)), TimeoutMs: 1000}).Return(
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
		tp := NewTopicPartition(topicName, partition)
		assert.Equal(t, len(testInputs), len(resp.ReceivedMessages))
		for index, msg := range resp.ReceivedMessages {
			ackMsg, _ := ParseAckID(msg.AckId)
			subImpl.Acknowledge(ctx, ackMsg, errChan)
			assert.Equal(t, testInputs[index].canConsumeMore, subImpl.CanConsumeMore())
			assert.Equal(t, testInputs[index].maxCommittedOffset, subImpl.consumedMessageStats[tp].maxCommittedOffset)
		}
	case <-time.NewTicker(tickerTimeout).C:
		assert.FailNow(t, "Test case timed out")
	}
}

func TestBasicImplementation_ModAckDeadline(t *testing.T) {
	ctx, cs, subImpl := setup(t)
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
			Topic:     topicName,
			Partition: partition,
		}
		msgProto.MessageID = input.messageID
		messages = append(messages, msgProto)
	}

	cs.EXPECT().ReceiveMessages(ctx, gomock.Any()).Return(
		&messagebroker.GetMessagesFromTopicResponse{
			Messages: messages,
		}, nil,
	)

	subscriber := getMockSubscriber(ctx, subImpl)
	pullRequest := &PullRequest{ctx: ctx, MaxNumOfMessages: 1}
	subImpl.Pull(ctx, pullRequest, subscriber.responseChan, subscriber.errChan)

	select {
	case resp := <-subscriber.responseChan:
		assert.NotEmpty(t, resp.ReceivedMessages)
		assert.Equal(t, len(testInputs), len(resp.ReceivedMessages))
		go subscriber.Run(ctx)
		for index, msg := range resp.ReceivedMessages {
			ackMsg, _ := ParseAckID(msg.AckId)
			modAckReq := &ModAckMessage{
				ctx:         ctx,
				AckMessage:  ackMsg,
				ackDeadline: testInputs[index].ackDeadline,
			}
			subscriber.modAckChan <- modAckReq
		}
	case <-time.NewTicker(tickerTimeout).C:
		assert.FailNow(t, "Test case timed out")
		return
	}

	tp := NewTopicPartition(topicName, partition)
	<-time.NewTimer(tickerTimeout).C
	subscriber.cancelFunc()
	assert.Zero(t, len(subImpl.consumedMessageStats[tp].consumedMessages))
}

func getMockSubscriber(ctx context.Context, subImpl *BasicImplementation) *Subscriber {
	_, cancelFunc := context.WithCancel(ctx)
	return &Subscriber{
		subscription:        subImpl.GetSubscription(),
		topic:               topicName,
		subscriberID:        subID,
		requestChan:         make(chan *PullRequest, 10),
		responseChan:        make(chan *metrov1.PullResponse, 10),
		errChan:             make(chan error, 1000),
		closeChan:           make(chan struct{}),
		modAckChan:          make(chan *ModAckMessage, 10),
		deadlineTicker:      time.NewTicker(1 * time.Second),
		healthMonitorTicker: time.NewTicker(1 * time.Minute),
		consumer:            subImpl.consumer,
		cancelFunc:          cancelFunc,
		ctx:                 ctx,
		subscriberImpl:      subImpl,
	}
}

func getMockBasicImplementation(ctx context.Context, consumer IConsumer, ctrl *gomock.Controller) *BasicImplementation {
	offsetRepo := mocks.NewMockIRepo(ctrl)
	offsetCore := offset.NewCore(offsetRepo)

	offsetRepo.EXPECT().Exists(gomock.Any(), gomock.Any()).AnyTimes()
	offsetRepo.EXPECT().Save(gomock.Any(), gomock.Any()).AnyTimes()

	return &BasicImplementation{
		maxOutstandingMessages: 1,
		maxOutstandingBytes:    100,
		topic:                  topicName,
		ctx:                    ctx,
		consumedMessageStats:   make(map[TopicPartition]*ConsumptionMetadata),
		subscriberID:           subID,
		consumer:               consumer,
		subscription: &subscription.Model{
			Name:  subName,
			Topic: topicName,
		},
		offsetCore: offsetCore,
	}
}

func getMockConsumerManager(
	ctx context.Context,
	ctrl *gomock.Controller,
	cs *mockMB.MockConsumer,
) IConsumer {
	bs := mockBS.NewMockIBrokerStore(ctrl)
	bs.EXPECT().GetConsumer(
		ctx,
		messagebroker.ConsumerClientOptions{
			Topics:          []string{topicName, retryTopic},
			GroupID:         subName,
			GroupInstanceID: subID,
			AutoOffsetReset: autoOffsetReset,
		},
	).Return(cs, nil)
	consumer, _ := NewConsumerManager(ctx, bs, 1000, subID, subName, topicName, retryTopic)
	return consumer
}

func getMockConsumer(ctx context.Context, ctrl *gomock.Controller) *mockMB.MockConsumer {
	cs := mockMB.NewMockConsumer(ctrl)
	req := messagebroker.GetTopicMetadataRequest{
		Topic:     topicName,
		Partition: partition,
	}
	cs.EXPECT().GetTopicMetadata(ctx, req).Return(messagebroker.GetTopicMetadataResponse{}, nil).AnyTimes()
	cs.EXPECT().Pause(ctx, gomock.Any()).AnyTimes().Return(nil)
	cs.EXPECT().CommitByPartitionAndOffset(gomock.Any(), gomock.Any()).Return(
		messagebroker.CommitOnTopicResponse{}, nil,
	).AnyTimes()
	return cs
}

func getMockReceivedMessages(input []string) []messagebroker.ReceivedMessage {
	messages := make([]messagebroker.ReceivedMessage, 0, len(input))
	for index, msg := range input {
		pubSub := &metrov1.PubsubMessage{Data: []byte(msg)}
		data, _ := proto.Marshal(pubSub)
		msgProto := messagebroker.ReceivedMessage{
			Data:       data,
			Topic:      topicName,
			Partition:  partition,
			Offset:     int32(index),
			Attributes: make([]map[string][]byte, 0, 1),
		}
		msgProto.MessageID = strconv.Itoa(index)
		msgProto.Attributes = append(msgProto.Attributes, map[string][]byte{
			uberTraceID: []byte(uuid.New().String()),
		})
		messages = append(messages, msgProto)
	}
	return messages
}

func TestBasicImplementation_GetConsumerLag(t *testing.T) {
	_, cs, subImpl := setup(t)
	pTopic := subImpl.topic
	rTopic := subImpl.subscription.GetRetryTopic()
	tests := []struct {
		name string
		want map[string]uint64
	}{
		{
			name: "Fetch consumer lag for consumer",
			want: map[string]uint64{
				pTopic: 0,
				rTopic: 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs.EXPECT().FetchConsumerLag(gomock.Any()).Return(map[string]uint64{
				pTopic: 0,
				rTopic: 0,
			}, nil).Times(1)
			got := subImpl.GetConsumerLag()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BasicImplementation.GetConsumerLag() = %v, want %v", got, tt.want)
			}
		})
	}
}
