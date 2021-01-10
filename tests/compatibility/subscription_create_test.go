// +build compatibility

package compatibility

import (
	"context"
	"testing"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
)

func Test_Subscription_CreateSubscription(t *testing.T) {
	ctx := context.Background()
	metroTopic, err := metroClient.CreateTopic(ctx, "topic-name-b")
	assert.Nil(t, err)

	emulatorTopic, err := emulatorClient.CreateTopic(ctx, "topic-name-b")
	assert.Nil(t, err)

	metroSub, err := metroClient.CreateSubscription(context.Background(), "sub-name-a",
		pubsub.SubscriptionConfig{Topic: metroTopic})
	assert.Nil(t, err)

	emulatorSub, err := emulatorClient.CreateSubscription(context.Background(), "sub-name-a",
		pubsub.SubscriptionConfig{Topic: emulatorTopic})
	assert.Nil(t, err)

	assert.Equal(t, emulatorSub.ID(), metroSub.ID())

	// cleanup
	err = metroTopic.Delete(ctx)
	assert.Nil(t, err)
	err = emulatorTopic.Delete(ctx)
	assert.Nil(t, err)
	err = metroSub.Delete(ctx)
	assert.Nil(t, err)
	err = emulatorSub.Delete(ctx)
	assert.Nil(t, err)
}

func Test_Subscription_CreateSubscription_InvalidSubscriptionName(t *testing.T) {
	ctx := context.Background()
	metroTopic, err := metroClient.CreateTopic(ctx, "topic-name-b")
	assert.Nil(t, err)

	emulatorTopic, err := emulatorClient.CreateTopic(ctx, "topic-name-b")
	assert.Nil(t, err)

	metroSub, errA := metroClient.CreateSubscription(context.Background(), "s",
		pubsub.SubscriptionConfig{Topic: metroTopic})
	assert.NotNil(t, errA)
	assert.Nil(t, metroSub)

	emulatorSub, errB := emulatorClient.CreateSubscription(context.Background(), "s",
		pubsub.SubscriptionConfig{Topic: emulatorTopic})
	assert.NotNil(t, errB)
	assert.Nil(t, emulatorSub)

	assert.Equal(t, errB.Error(), errA.Error())

	// cleanup
	err = metroTopic.Delete(ctx)
	assert.Nil(t, err)
	err = emulatorTopic.Delete(ctx)
	assert.Nil(t, err)
}
