// +build compatibility

package compatibility

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_Pubsub(t *testing.T) {
	topicName := fmt.Sprintf("topic-%s", uuid.New().String()[0:4])
	subscription := fmt.Sprintf("sub-%s", uuid.New().String()[0:4])

	// This is just a placeholder test.
	// TODO: fix it with more tests and better structure
	for k, client := range []*pubsub.Client{metroClient, emulatorClient} {
		t.Logf("running index %d", k)
		topic, err := client.CreateTopic(context.Background(), topicName)
		assert.Nil(t, err)
		assert.NotNil(t, topic)

		sub, err := client.CreateSubscription(context.Background(), subscription,
			pubsub.SubscriptionConfig{Topic: topic})
		assert.Nil(t, err)

		ctx, cancelFunc := context.WithCancel(context.Background())

		//sub1.ReceiveSettings.Synchronous = true
		sub.ReceiveSettings.NumGoroutines = 1
		go sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			t.Logf("[%d] Got message: %q\n", k, string(m.Data))
			m.Ack()
		})
		//topic.EnableMessageOrdering = true
		for i := 0; i < 10; i++ {
			r := topic.Publish(context.Background(), &pubsub.Message{Data: []byte(fmt.Sprintf("payload %d", i))})
			r.Get(context.Background())
		}

		topic.Stop()

		time.Sleep(5 * time.Second)

		// cleanup
		err = topic.Delete(ctx)
		assert.Nil(t, err)
		err = sub.Delete(ctx)
		assert.Nil(t, err)

		cancelFunc()
	}
}
