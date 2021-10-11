package subscriber

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	mockBS "github.com/razorpay/metro/internal/brokerstore/mocks"
	"github.com/razorpay/metro/pkg/messagebroker"
	mockMB "github.com/razorpay/metro/pkg/messagebroker/mocks"
	"github.com/stretchr/testify/assert"
)

func TestConsumerManager_ResumeWithoutPause(t *testing.T) {
	ctrl := gomock.NewController(t)
	bs := mockBS.NewMockIBrokerStore(ctrl)
	cs := mockMB.NewMockConsumer(ctrl)
	ctx := context.Background()

	subID := "subscriber-id"
	subName := "subscription-name"
	topic := "primary-topic"
	retryTopic := "retry-topic"

	bs.EXPECT().GetConsumer(
		ctx,
		messagebroker.ConsumerClientOptions{
			Topics:          []string{topic, retryTopic},
			GroupID:         subName,
			GroupInstanceID: subID,
		},
	).Return(cs, nil)
	consumer, _ := NewConsumerManager(ctx, bs, subID, subName, topic, retryTopic)

	e := consumer.ResumeConsumer(ctx)
	assert.Equal(t, e, cannotResume)
	e = consumer.ResumePrimaryConsumer(ctx)
	assert.Equal(t, e, cannotResume)
}

func TestConsumerManager_PauseConsumer(t *testing.T) {
	ctrl := gomock.NewController(t)
	bs := mockBS.NewMockIBrokerStore(ctrl)
	cs := mockMB.NewMockConsumer(ctrl)
	ctx := context.Background()

	subID := "subscriber-id"
	subName := "subscription-name"
	topic := "primary-topic"
	retryTopic := "retry-topic"

	bs.EXPECT().GetConsumer(
		ctx,
		messagebroker.ConsumerClientOptions{
			Topics:          []string{topic, retryTopic},
			GroupID:         subName,
			GroupInstanceID: subID,
		},
	).Return(cs, nil)
	consumer, _ := NewConsumerManager(ctx, bs, subID, subName, topic, retryTopic)

	cs.EXPECT().Pause(ctx, gomock.Any()).Times(2).Return(nil)
	cs.EXPECT().Resume(ctx, gomock.Any()).Times(2).Return(nil)

	e := consumer.PauseConsumer(ctx)
	assert.Nil(t, e)
	assert.True(t, consumer.IsPaused(ctx))
	assert.False(t, consumer.IsPrimaryPaused(ctx))

	e = consumer.ResumeConsumer(ctx)
	assert.Nil(t, e)
	assert.False(t, consumer.IsPaused(ctx))
	assert.False(t, consumer.IsPrimaryPaused(ctx))
}

func TestConsumerManager_PausePrimaryConsumer(t *testing.T) {
	ctrl := gomock.NewController(t)
	bs := mockBS.NewMockIBrokerStore(ctrl)
	cs := mockMB.NewMockConsumer(ctrl)
	ctx := context.Background()

	subID := "subscriber-id"
	subName := "subscription-name"
	topic := "primary-topic"
	retryTopic := "retry-topic"

	bs.EXPECT().GetConsumer(
		ctx,
		messagebroker.ConsumerClientOptions{
			Topics:          []string{topic, retryTopic},
			GroupID:         subName,
			GroupInstanceID: subID,
		},
	).Return(cs, nil)
	consumer, _ := NewConsumerManager(ctx, bs, subID, subName, topic, retryTopic)

	cs.EXPECT().Pause(ctx, gomock.Any()).Times(1).Return(nil)
	cs.EXPECT().Resume(ctx, gomock.Any()).Times(1).Return(nil)

	e := consumer.PausePrimaryConsumer(ctx)
	assert.Nil(t, e)
	assert.False(t, consumer.IsPaused(ctx))
	assert.True(t, consumer.IsPrimaryPaused(ctx))

	e = consumer.ResumePrimaryConsumer(ctx)
	assert.Nil(t, e)
	assert.False(t, consumer.IsPaused(ctx))
	assert.False(t, consumer.IsPrimaryPaused(ctx))
}

func TestConsumerManager_PausePrimaryAfterConsumer(t *testing.T) {
	ctrl := gomock.NewController(t)
	bs := mockBS.NewMockIBrokerStore(ctrl)
	cs := mockMB.NewMockConsumer(ctrl)
	ctx := context.Background()

	subID := "subscriber-id"
	subName := "subscription-name"
	topic := "primary-topic"
	retryTopic := "retry-topic"

	bs.EXPECT().GetConsumer(
		ctx,
		messagebroker.ConsumerClientOptions{
			Topics:          []string{topic, retryTopic},
			GroupID:         subName,
			GroupInstanceID: subID,
		},
	).Return(cs, nil)
	consumer, _ := NewConsumerManager(ctx, bs, subID, subName, topic, retryTopic)

	cs.EXPECT().Pause(ctx, gomock.Any()).Times(2).Return(nil)
	cs.EXPECT().Resume(ctx, gomock.Any()).Times(2).Return(nil)

	consumer.PauseConsumer(ctx)
	consumer.PausePrimaryConsumer(ctx)

	assert.True(t, consumer.IsPaused(ctx))
	assert.True(t, consumer.IsPrimaryPaused(ctx))

	consumer.ResumePrimaryConsumer(ctx)
	assert.False(t, consumer.IsPrimaryPaused(ctx))
	assert.True(t, consumer.IsPaused(ctx))

	consumer.ResumeConsumer(ctx)
	assert.False(t, consumer.IsPrimaryPaused(ctx))
	assert.False(t, consumer.IsPaused(ctx))
}

func TestConsumerManager_PauseConsumerAfterPrimary(t *testing.T) {
	ctrl := gomock.NewController(t)
	bs := mockBS.NewMockIBrokerStore(ctrl)
	cs := mockMB.NewMockConsumer(ctrl)
	ctx := context.Background()

	subID := "subscriber-id"
	subName := "subscription-name"
	topic := "primary-topic"
	retryTopic := "retry-topic"

	bs.EXPECT().GetConsumer(
		ctx,
		messagebroker.ConsumerClientOptions{
			Topics:          []string{topic, retryTopic},
			GroupID:         subName,
			GroupInstanceID: subID,
		},
	).Return(cs, nil)
	consumer, _ := NewConsumerManager(ctx, bs, subID, subName, topic, retryTopic)

	cs.EXPECT().Pause(ctx, gomock.Any()).Times(2).Return(nil)
	cs.EXPECT().Resume(ctx, gomock.Any()).Times(2).Return(nil)

	consumer.PausePrimaryConsumer(ctx)
	consumer.PauseConsumer(ctx)

	assert.True(t, consumer.IsPaused(ctx))
	assert.True(t, consumer.IsPrimaryPaused(ctx))

	consumer.ResumeConsumer(ctx)
	assert.True(t, consumer.IsPrimaryPaused(ctx))
	assert.False(t, consumer.IsPaused(ctx))

	consumer.ResumePrimaryConsumer(ctx)
	assert.False(t, consumer.IsPrimaryPaused(ctx))
	assert.False(t, consumer.IsPaused(ctx))
}

func TestConsumerManager_Commit(t *testing.T) {
	ctrl := gomock.NewController(t)
	bs := mockBS.NewMockIBrokerStore(ctrl)
	cs := mockMB.NewMockConsumer(ctrl)
	ctx := context.Background()

	subID := "subscriber-id"
	subName := "subscription-name"
	topic := "primary-topic"
	retryTopic := "retry-topic"

	bs.EXPECT().GetConsumer(
		ctx,
		messagebroker.ConsumerClientOptions{
			Topics:          []string{topic, retryTopic},
			GroupID:         subName,
			GroupInstanceID: subID,
		},
	).Return(cs, nil)
	consumer, _ := NewConsumerManager(ctx, bs, subID, subName, topic, retryTopic)

	req := messagebroker.CommitOnTopicRequest{Topic: topic}
	resp := messagebroker.CommitOnTopicResponse{}
	cs.EXPECT().CommitByPartitionAndOffset(ctx, req).Times(1).Return(resp, nil)

	r, e := consumer.CommitByPartitionAndOffset(ctx, req)
	assert.Nil(t, e)
	assert.Equal(t, resp, r)
}

func TestConsumerManager_Receive(t *testing.T) {
	ctrl := gomock.NewController(t)
	bs := mockBS.NewMockIBrokerStore(ctrl)
	cs := mockMB.NewMockConsumer(ctrl)
	ctx := context.Background()

	subID := "subscriber-id"
	subName := "subscription-name"
	topic := "primary-topic"
	retryTopic := "retry-topic"

	bs.EXPECT().GetConsumer(
		ctx,
		messagebroker.ConsumerClientOptions{
			Topics:          []string{topic, retryTopic},
			GroupID:         subName,
			GroupInstanceID: subID,
		},
	).Return(cs, nil)
	consumer, _ := NewConsumerManager(ctx, bs, subID, subName, topic, retryTopic)

	req := messagebroker.GetMessagesFromTopicRequest{}
	resp := &messagebroker.GetMessagesFromTopicResponse{}
	cs.EXPECT().ReceiveMessages(ctx, req).Times(1).Return(resp, nil)

	r, e := consumer.ReceiveMessages(ctx, req)
	assert.Nil(t, e)
	assert.Equal(t, resp, r)
}

func TestConsumerManager_Metadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	bs := mockBS.NewMockIBrokerStore(ctrl)
	cs := mockMB.NewMockConsumer(ctrl)
	ctx := context.Background()

	subID := "subscriber-id"
	subName := "subscription-name"
	topic := "primary-topic"
	retryTopic := "retry-topic"

	bs.EXPECT().GetConsumer(
		ctx,
		messagebroker.ConsumerClientOptions{
			Topics:          []string{topic, retryTopic},
			GroupID:         subName,
			GroupInstanceID: subID,
		},
	).Return(cs, nil)
	consumer, _ := NewConsumerManager(ctx, bs, subID, subName, topic, retryTopic)

	req := messagebroker.GetTopicMetadataRequest{}
	resp := messagebroker.GetTopicMetadataResponse{}
	cs.EXPECT().GetTopicMetadata(ctx, req).Times(1).Return(resp, nil)

	r, e := consumer.GetTopicMetadata(ctx, req)
	assert.Nil(t, e)
	assert.Equal(t, resp, r)
}

func TestConsumerManager_Close(t *testing.T) {
	ctrl := gomock.NewController(t)
	bs := mockBS.NewMockIBrokerStore(ctrl)
	cs := mockMB.NewMockConsumer(ctrl)
	ctx := context.Background()

	subID := "subscriber-id"
	subName := "subscription-name"
	topic := "primary-topic"
	retryTopic := "retry-topic"

	bs.EXPECT().GetConsumer(
		ctx,
		messagebroker.ConsumerClientOptions{
			Topics:          []string{topic, retryTopic},
			GroupID:         subName,
			GroupInstanceID: subID,
		},
	).Return(cs, nil)

	bs.EXPECT().RemoveConsumer(
		ctx,
		messagebroker.ConsumerClientOptions{
			GroupID:         subName,
			GroupInstanceID: subID,
		},
	).Return(true)

	cs.EXPECT().Close(ctx).Times(1).Return(nil)

	consumer, _ := NewConsumerManager(ctx, bs, subID, subName, topic, retryTopic)
	e := consumer.Close(ctx)
	assert.Nil(t, e)

}
