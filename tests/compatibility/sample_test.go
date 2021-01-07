// +build compatibility

package compatibility

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
)

func Test_Pubsub(t *testing.T) {
	// This is just a placeholder test.
	// TODO: fix it with more tests and better structure
	for k, client := range []*pubsub.Client{metroClient, emulatorClient} {
		topic, err := client.CreateTopic(context.Background(), "topic-name")
		assert.Nil(t, err)
		assert.NotNil(t, topic)

		r := topic.Publish(context.Background(), &pubsub.Message{Data: []byte("payload")})

		r.Get(context.Background())
		topic.Stop()

		sub, err := client.CreateSubscription(context.Background(), "sub-name",
			pubsub.SubscriptionConfig{Topic: topic})
		fmt.Println(k)

		assert.Nil(t, err)

		sub.ReceiveSettings.Synchronous = true
		ctx, cancelFunc := context.WithCancel(context.Background())

		go sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			//m.Ack()
		})

		time.Sleep(20 * time.Millisecond)
		cancelFunc()
	}
}
