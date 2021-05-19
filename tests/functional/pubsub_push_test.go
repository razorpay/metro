// +build functional

package functional

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_PubSubPush(t *testing.T) {
	// create a topic
	topicName := fmt.Sprintf("topic-%s", uuid.New().String()[0:4])
	topic, err := client.CreateTopic(context.Background(), topicName)
	assert.Nil(t, err)
	assert.NotNil(t, topic)

	// create a push subscription
	subscription := fmt.Sprintf("sub-%s", uuid.New().String()[0:4])
	sub, err := client.CreateSubscription(context.Background(), subscription,
		pubsub.SubscriptionConfig{
			Topic: topic,
			PushConfig: pubsub.PushConfig{
				Endpoint: mockServerPushEndpoint,
			},
		})
	if err != nil {
		t.Logf("error in create subscription: %s", err)
	}
	assert.Nil(t, err)

	// Adding some delay for subscription scheduling
	// Explore solutions
	// 1. Subscription State
	// 2. Node Binding API
	// 3. Read from registry directly in tests
	time.Sleep(time.Duration(time.Second * 5))

	ctx := context.Background()

	// publish messages
	numOfMessages := 10
	for i := 0; i < numOfMessages; i++ {
		text := fmt.Sprintf("payload %d", i)
		r := topic.Publish(ctx, &pubsub.Message{Data: []byte(text)})
		r.Get(ctx)
	}
	topic.Stop()

	// validations

	// cleanup
	err = topic.Delete(ctx)
	assert.Nil(t, err)
	err = sub.Delete(ctx)
	assert.Nil(t, err)
}
