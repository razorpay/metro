// +build compatibility

package compatibility

import (
	"context"
	"fmt"
	"testing"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_Subscription_CreateSubscription(t *testing.T) {
	topic := fmt.Sprintf("topic-%s", uuid.New().String()[0:4])
	subscription := fmt.Sprintf("sub-%s", uuid.New().String()[0:4])

	ctx := context.Background()
	metroTopic, err := metroClient.CreateTopic(ctx, topic)
	assert.Nil(t, err)

	emulatorTopic, err := emulatorClient.CreateTopic(ctx, topic)
	assert.Nil(t, err)

	metroSub, err := metroClient.CreateSubscription(context.Background(), subscription,
		pubsub.SubscriptionConfig{Topic: metroTopic})
	assert.Nil(t, err)

	emulatorSub, err := emulatorClient.CreateSubscription(context.Background(), subscription,
		pubsub.SubscriptionConfig{Topic: emulatorTopic})
	assert.Nil(t, err)

	assert.Equal(t, emulatorSub.ID(), metroSub.ID())
	assert.Equal(t, emulatorSub.ReceiveSettings, metroSub.ReceiveSettings)
	assert.Equal(t, emulatorSub.String(), metroSub.String())

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
	topic := fmt.Sprintf("topic-%s", uuid.New().String()[0:4])

	ctx := context.Background()
	metroTopic, err := metroClient.CreateTopic(ctx, topic)
	assert.Nil(t, err)

	emulatorTopic, err := emulatorClient.CreateTopic(ctx, topic)
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
