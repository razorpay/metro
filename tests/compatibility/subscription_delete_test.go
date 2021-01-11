// +build compatibility

package compatibility

import (
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_Subscription_DeleteSubscription(t *testing.T) {
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

	err = metroSub.Delete(ctx)
	assert.Nil(t, err)
	err = emulatorSub.Delete(ctx)
	assert.Nil(t, err)

	// delete already deleted subscriptions
	errA := metroSub.Delete(ctx)
	assert.NotNil(t, errA)
	errB := emulatorSub.Delete(ctx)
	assert.NotNil(t, errB)
	assert.Equal(t, errB.Error(), errA.Error())

	// cleanup
	err = metroTopic.Delete(ctx)
	assert.Nil(t, err)
	err = emulatorTopic.Delete(ctx)
	assert.Nil(t, err)
}
