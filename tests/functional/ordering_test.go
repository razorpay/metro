// +build functional

package functional

import (
	"context"
	"fmt"
	"testing"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
)

func Test_Ordering_NoPushFailure(t *testing.T) {
	topic := client.Topic(orderedTopic)
	topic.EnableMessageOrdering = true
	subName := fmt.Sprintf("projects/%s/subscriptions/%s", projectId, orderedSub)
	pushChan := chanMap[subName]

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		_, err := topic.Publish(ctx, &pubsub.Message{Data: []byte("data"), OrderingKey: "o1"}).Get(ctx)
		assert.Nil(t, err)
	}

	for i := 0; i < 3; i++ {
		pushMessage := <-pushChan
		pushMessage.ResponseChan <- 200
	}
}

func Test_Ordering_FailDelivery(t *testing.T) {
	topic := client.Topic(orderedTopic)
	topic.EnableMessageOrdering = true
	subName := fmt.Sprintf("projects/%s/subscriptions/%s", projectId, orderedSub)
	pushChan := chanMap[subName]

	ctx := context.Background()

	orderedMessages := map[string][]string{
		"o1": []string{"m1", "m2", "m3"},
	}

	expectedDelivery := map[string][]string{
		"o1": []string{"m1", "m1", "m2", "m2", "m3", "m3"},
	}

	for orderKey, msgNos := range orderedMessages {
		for _, msgNo := range msgNos {
			_, err := topic.Publish(
				ctx,
				&pubsub.Message{
					Data:        []byte(""),
					OrderingKey: orderKey,
					Attributes: map[string]string{
						"order": msgNo,
					},
				},
			).Get(ctx)
			assert.Nil(t, err)
		}
	}

	for i := 0; i < 6; i++ {
		pushMessage := <-pushChan
		pushMessage.ResponseChan <- 500
		msgNos := expectedDelivery[pushMessage.Request.Message.OrderingKey]
		assert.NotZero(t, msgNos)
		assert.Equal(t, msgNos[0], pushMessage.Request.Message.Attributes["order"])
		expectedDelivery[pushMessage.Request.Message.OrderingKey] = msgNos[1:]
	}
}

func Test_Ordering_FailDeliveryNoOrderKey(t *testing.T) {
	topic := client.Topic(orderedTopic)
	topic.EnableMessageOrdering = true
	subName := fmt.Sprintf("projects/%s/subscriptions/%s", projectId, orderedSub)
	pushChan := chanMap[subName]

	ctx := context.Background()

	type orderingMeta struct {
		Key   string
		MsgNo string
	}
	orderedMessages := map[string][]string{
		"o1": []string{"m1", "m2"},
		"":   []string{"mx"},
	}

	expectedDelivery := map[string][]string{
		"o1": []string{"m1", "m1", "m2", "m2"},
		"":   []string{"mx", "mx"},
	}

	for orderKey, msgNos := range orderedMessages {
		for _, msgNo := range msgNos {
			_, err := topic.Publish(
				ctx,
				&pubsub.Message{
					Data:        []byte(""),
					OrderingKey: orderKey,
					Attributes: map[string]string{
						"order": msgNo,
					},
				},
			).Get(ctx)
			assert.Nil(t, err)
		}
	}

	for i := 0; i < 6; i++ {
		pushMessage := <-pushChan
		pushMessage.ResponseChan <- 500
		msgNos := expectedDelivery[pushMessage.Request.Message.OrderingKey]
		assert.NotZero(t, msgNos)
		assert.Equal(t, msgNos[0], pushMessage.Request.Message.Attributes["order"])
		expectedDelivery[pushMessage.Request.Message.OrderingKey] = msgNos[1:]
	}
}

func Test_Ordering_FailDelivery_MultipleKeys(t *testing.T) {
	topic := client.Topic(orderedTopic)
	topic.EnableMessageOrdering = true
	subName := fmt.Sprintf("projects/%s/subscriptions/%s", projectId, orderedSub)
	pushChan := chanMap[subName]

	ctx := context.Background()

	orderedMessages := map[string][]string{
		"o1": []string{"m1", "m2"},
		"o2": []string{"m1", "m2"},
	}

	expectedDelivery := map[string][]string{
		"o1": []string{"m1", "m1", "m2", "m2"},
		"o2": []string{"m1", "m1", "m2", "m2"},
	}

	for orderKey, msgNos := range orderedMessages {
		for _, msgNo := range msgNos {
			_, err := topic.Publish(
				ctx,
				&pubsub.Message{
					Data:        []byte(""),
					OrderingKey: orderKey,
					Attributes: map[string]string{
						"order": msgNo,
					},
				},
			).Get(ctx)
			assert.Nil(t, err)
		}
	}

	for i := 0; i < 6; i++ {
		pushMessage := <-pushChan
		pushMessage.ResponseChan <- 500
		msgNos := expectedDelivery[pushMessage.Request.Message.OrderingKey]
		assert.NotZero(t, msgNos)
		assert.Equal(t, msgNos[0], pushMessage.Request.Message.Attributes["order"])
		expectedDelivery[pushMessage.Request.Message.OrderingKey] = msgNos[1:]
	}
}

func Test_Ordering_Filtered(t *testing.T) {
	topic := client.Topic(orderedFilterTopic)
	topic.EnableMessageOrdering = true
	subName := fmt.Sprintf("projects/%s/subscriptions/%s", projectId, orderedFilterSub)
	pushChan := chanMap[subName]

	ctx := context.Background()

	orderedMessages := map[string][]string{
		"o1": []string{"m1", "m2", "m3"},
	}
	attributes := []string{"org", "not org", "org"}

	expectedDelivery := map[string][]string{
		"o1": []string{"m1", "m1", "m3", "m3"},
	}

	for orderKey, msgNos := range orderedMessages {
		for i, msgNo := range msgNos {
			_, err := topic.Publish(
				ctx,
				&pubsub.Message{
					Data:        []byte(""),
					OrderingKey: orderKey,
					Attributes: map[string]string{
						"order": msgNo,
						"x":     attributes[i],
						"uber-trace-id": "1f9a3064b9adbfef:74f7e093c1eaac41:759efc8483a32b97:0"
					},
				},
			).Get(ctx)
			assert.Nil(t, err)
		}
	}

	for i := 0; i < 4; i++ {
		pushMessage := <-pushChan
		pushMessage.ResponseChan <- 500
		msgNos := expectedDelivery[pushMessage.Request.Message.OrderingKey]
		assert.NotZero(t, msgNos)
		assert.Equal(t, msgNos[0], pushMessage.Request.Message.Attributes["order"])
		expectedDelivery[pushMessage.Request.Message.OrderingKey] = msgNos[1:]
	}
}
