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

	t.Log("Pushing")
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		_, err := topic.Publish(ctx, &pubsub.Message{Data: []byte(""), OrderingKey: "o1"}).Get(ctx)
		assert.Nil(t, err)
	}

	for i := 0; i < 3; i++ {
		t.Log("Waiting for push")
		pushMessage := <-pushChan
		pushMessage.ResponseChan <- 200
	}
}
