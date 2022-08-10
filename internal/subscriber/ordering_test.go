package subscriber

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

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

var mockBatchSize int32 = 1
var mockOffset int32 = 3

func orderedSetup(t *testing.T, lastStatus string) (
	ctx context.Context,
	subImpl *OrderedImplementation,
	cs *mockMB.MockConsumer,
) {
	ctrl := gomock.NewController(t)
	ctx = context.Background()
	cs = getMockConsumer(ctx, ctrl)
	cs.EXPECT().ReceiveMessages(ctx, messagebroker.GetMessagesFromTopicRequest{NumOfMessages: mockBatchSize, TimeoutMs: 1000}).Return(
		&messagebroker.GetMessagesFromTopicResponse{
			Messages: getDummyOrderedReceivedMessage(),
		}, nil,
	)
	consumer := getMockConsumerManager(ctx, ctrl, cs)
	offsetRepo := getMockOffsetRepo(ctrl, lastStatus)
	subImpl = getMockOrderedImplementation(ctx, consumer, offsetRepo)
	return
}

func TestOrderedImplementation_Pull(t *testing.T) {
	ctx, subImpl, _ := orderedSetup(t, string(sequenceFailure))
	pullRequest := &PullRequest{ctx: ctx, MaxNumOfMessages: mockBatchSize}
	responseChan := make(chan *metrov1.PullResponse, 10)
	errChan := make(chan error, 10)
	subImpl.Pull(ctx, pullRequest, responseChan, errChan)

	select {
	case resp := <-responseChan:
		assert.Zero(t, len(resp.ReceivedMessages))
		assert.True(t, subImpl.consumer.IsPrimaryPaused(ctx))
	case <-time.NewTicker(tickerTimeout).C:
		assert.FailNow(t, "Test case timed out")
	}
}

func TestOrderedImplementation_Acknowledge(t *testing.T) {
	ctx, subImpl, _ := orderedSetup(t, string(sequenceSuccess))
	pullRequest := &PullRequest{ctx: ctx, MaxNumOfMessages: mockBatchSize}
	responseChan := make(chan *metrov1.PullResponse, 10)
	errChan := make(chan error, 10)
	subImpl.Pull(ctx, pullRequest, responseChan, errChan)
	select {
	case resp := <-responseChan:
		tp := NewTopicPartition(topicName, partition)
		for _, msg := range resp.ReceivedMessages {
			ackMsg, _ := ParseAckID(msg.AckId)
			subImpl.Acknowledge(ctx, ackMsg, errChan)
		}
		assert.Equal(t, mockOffset, subImpl.consumedMessageStats[tp].maxCommittedOffset)
	case <-time.NewTicker(tickerTimeout).C:
		assert.FailNow(t, "Test case timed out")
	}
}

func TestOrderedImplementation_ModAckDeadline(t *testing.T) {
	ctx, subImpl, _ := orderedSetup(t, string(sequenceSuccess))
	pullRequest := &PullRequest{ctx: ctx, MaxNumOfMessages: mockBatchSize}
	responseChan := make(chan *metrov1.PullResponse, 10)
	errChan := make(chan error, 10)
	subImpl.Pull(ctx, pullRequest, responseChan, errChan)

	select {
	case resp := <-responseChan:
		assert.NotEmpty(t, resp.ReceivedMessages)
		for _, msg := range resp.ReceivedMessages {
			ackMsg, _ := ParseAckID(msg.AckId)
			modAckReq := &ModAckMessage{
				ctx:         ctx,
				AckMessage:  ackMsg,
				ackDeadline: int32(0),
			}
			subImpl.ModAckDeadline(ctx, modAckReq, errChan)
		}
	case <-time.NewTicker(tickerTimeout).C:
		assert.FailNow(t, "Test case timed out")
		return
	}
	tp := NewTopicPartition(topicName, partition)
	assert.Zero(t, len(subImpl.consumedMessageStats[tp].consumedMessages))
}

func TestOrderedImplementation_GetConsumerLag(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := context.Background()
	cs := getMockConsumer(ctx, ctrl)
	consumer := getMockConsumerManager(ctx, ctrl, cs)
	offsetRepo := getMockOffsetRepo(ctrl, string(sequenceSuccess))
	subImpl := getMockOrderedImplementation(ctx, consumer, offsetRepo)
	pTopic := subImpl.topic
	rTopic := subImpl.subscription.GetRetryTopic()
	tests := []struct {
		name string
		want map[string]uint64
	}{
		{
			name: "Get Consumer Lag for Ordered Implementation",
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

func TestOrderedImplementation_EvictUnackedMessagesPastDeadline(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := context.Background()
	cs := getMockConsumer(ctx, ctrl)
	consumer := getMockConsumerManager(ctx, ctrl, cs)
	offsetRepo := getMockOffsetRepo(ctrl, string(sequenceSuccess))
	subImpl := getMockOrderedImplementation(ctx, consumer, offsetRepo)
	orderedConsumptionMetadata := NewOrderedConsumptionMetadata()
	orderedConsumptionMetadata.Store(getReceivedMessage(), time.Now().Add(time.Second*(-1)).Unix())
	subImpl.consumedMessageStats[NewTopicPartition(topicName, partition)] = orderedConsumptionMetadata
	pTopic := subImpl.topic
	rTopic := subImpl.subscription.GetRetryTopic()
	tests := []struct {
		name    string
		want    map[string]uint64
		wantErr bool
	}{
		{
			name: "Test Evict Unacked Messages past Deadline",
			want: map[string]uint64{
				pTopic: 0,
				rTopic: 0,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errorCh := make(chan error)
			go subImpl.EvictUnackedMessagesPastDeadline(ctx, errorCh)
			select {
			case err := <-errorCh:
				if !tt.wantErr && err != nil {
					t.Errorf("Got Error while Evicting : %v", err)
				}
			case <-time.NewTicker(time.Second * 1).C:
				fmt.Println(subImpl.consumedMessageStats)
				assert.Equal(t, len(subImpl.consumedMessageStats[NewTopicPartition(topicName, partition)].ConsumptionMetadata.consumedMessages), 0)
			}
		})
	}
}

func getMockOrderedImplementation(
	ctx context.Context,
	consumer IConsumer,
	offsetRepo *mocks.MockIRepo,
) *OrderedImplementation {
	offsetCore := offset.NewCore(offsetRepo)

	return &OrderedImplementation{
		maxOutstandingMessages: 1,
		maxOutstandingBytes:    100,
		topic:                  topicName,
		subscriberID:           subID,
		consumer:               consumer,
		offsetCore:             offsetCore,
		ctx:                    ctx,
		subscription: &subscription.Model{
			Name:  subName,
			Topic: topicName,
			DeadLetterPolicy: &subscription.DeadLetterPolicy{
				MaxDeliveryAttempts: 3,
			},
		},
		consumedMessageStats: make(map[TopicPartition]*OrderedConsumptionMetadata),
		pausedMessages:       make([]messagebroker.ReceivedMessage, 0),
		sequenceManager:      NewOffsetSequenceManager(ctx, offsetCore),
	}
}

func getDummyOrderedReceivedMessage() []messagebroker.ReceivedMessage {
	pubSub := &metrov1.PubsubMessage{Data: []byte("a")}
	data, _ := proto.Marshal(pubSub)
	msgProto := messagebroker.ReceivedMessage{
		Data:        data,
		Topic:       topicName,
		Partition:   partition,
		Offset:      mockOffset,
		OrderingKey: orderingKey,
	}
	msgProto.MessageID = "1"
	return []messagebroker.ReceivedMessage{msgProto}
}

func getReceivedMessage() messagebroker.ReceivedMessage {
	pubSub := &metrov1.PubsubMessage{Data: []byte("a")}
	data, _ := proto.Marshal(pubSub)
	msgProto := messagebroker.ReceivedMessage{
		Data:        data,
		Topic:       topicName,
		Partition:   partition,
		Offset:      1,
		OrderingKey: orderingKey,
	}
	msgProto.MessageID = "1"
	return msgProto
}

func getMockOffsetRepo(ctrl *gomock.Controller, status string) *mocks.MockIRepo {
	offsetRepo := mocks.NewMockIRepo(ctrl)

	offsetRepo.EXPECT().Exists(gomock.Any(), gomock.Any()).AnyTimes().Return(true, nil)
	offsetRepo.EXPECT().Save(gomock.Any(), gomock.Any()).AnyTimes()
	offsetRepo.EXPECT().Delete(gomock.Any(), gomock.Any()).AnyTimes()
	offsetRepo.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.AssignableToTypeOf(&offset.Model{})).DoAndReturn(
		func(arg1 context.Context, arg2 string, arg3 *offset.Model) *offset.Model {
			arg3.LatestOffset = mockOffset - 1
			return arg3
		}).AnyTimes()

	offsetRepo.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.AssignableToTypeOf(&offset.Status{})).DoAndReturn(
		func(arg1 context.Context, arg2 string, arg3 *offset.Status) *offset.Status {
			arg3.OffsetStatus = status
			arg3.LatestOffset = mockOffset - 1
			return arg3
		}).AnyTimes()
	return offsetRepo
}
