// +build functional

package functional

import (
	"context"
	"fmt"
	"testing"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_CreatePullSubscription(t *testing.T) {
	// create a topic
	topicName := fmt.Sprintf("topic-%s", uuid.New().String()[0:4])
	topic, err := client.CreateTopic(context.Background(), topicName)
	assert.Nil(t, err)
	assert.NotNil(t, topic)

	// create a pull subscription
	subscription := fmt.Sprintf("sub-%s", uuid.New().String()[0:4])
	sub, err := client.CreateSubscription(context.Background(), subscription,
		pubsub.SubscriptionConfig{
			Topic: topic,
		})
	if err != nil {
		t.Logf("error in create subscription: %s", err)
	}
	assert.Nil(t, err)

	// cleanup
	ctx := context.Background()
	err = topic.Delete(ctx)
	assert.Nil(t, err)
	err = sub.Delete(ctx)
	assert.Nil(t, err)
}

func Test_CreateDeadLetterTopicSubscription(t *testing.T) {
	// create a topic
	topicName := fmt.Sprintf("topic-%s", uuid.New().String()[0:4])
	topic, err := client.CreateTopic(context.Background(), topicName)
	assert.Nil(t, err)
	assert.NotNil(t, topic)

	// create a pull subscription
	subscription := fmt.Sprintf("sub-%s", uuid.New().String()[0:4])
	sub, err := client.CreateSubscription(context.Background(), subscription,
		pubsub.SubscriptionConfig{
			Topic: topic,
		})
	if err != nil {
		t.Logf("error in create subscription: %s", err)
	}
	assert.Nil(t, err)

	// create a pull subscription on dead letter topic
	dltopicName := subscription + "-dlq"
	subscription = fmt.Sprintf("sub-%s", uuid.New().String()[0:4])
	sub2, err := client.CreateSubscription(context.Background(), subscription,
		pubsub.SubscriptionConfig{
			Topic: client.Topic(dltopicName),
		})
	if err != nil {
		t.Logf("error in create subscription: %s", err)
	}
	assert.Nil(t, err)

	// cleanup
	ctx := context.Background()
	err = sub.Delete(ctx)
	assert.Nil(t, err)
	err = sub2.Delete(ctx)
	assert.Nil(t, err)
	err = topic.Delete(ctx)
	assert.Nil(t, err)
}

func Test_CreatePushSubscription(t *testing.T) {
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
				Endpoint: "http://localhost:8099/push",
			},
		})

	assert.Nil(t, err)

	// cleanup
	ctx := context.Background()
	err = topic.Delete(ctx)
	assert.Nil(t, err)
	err = sub.Delete(ctx)
	assert.Nil(t, err)
}

func Test_CreateSubscriptionInvalidTopic(t *testing.T) {
	// create a push subscription
	subscription := fmt.Sprintf("sub-%s", uuid.New().String()[0:4])
	topicName := fmt.Sprintf("topic-%s", uuid.New().String()[0:4])
	_, err := client.CreateSubscription(context.Background(), subscription,
		pubsub.SubscriptionConfig{
			Topic: client.Topic(topicName),
		})

	assert.NotNil(t, err)
}
