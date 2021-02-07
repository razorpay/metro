// +build compatibility

package compatibility

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
)

func Test_Pubsub(t *testing.T) {
	// This is just a placeholder test.
	// TODO: fix it with more tests and better structure
	for k, client := range []*pubsub.Client{metroClient, emulatorClient} {
		t.Logf("running index %d", k)
		topic, err := client.CreateTopic(context.Background(), "topic-name")
		if err != nil {
			t.Logf("error creating topic %s", err.Error())
		}
		assert.Nil(t, err)
		assert.NotNil(t, topic)

		sub, err := client.CreateSubscription(context.Background(), "sub-name",
			pubsub.SubscriptionConfig{Topic: topic})

		assert.Nil(t, err)

		ctx, cancelFunc := context.WithCancel(context.Background())

		//sub1.ReceiveSettings.Synchronous = true

		go sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			t.Logf("[%d] Got message: %q\n", k, string(m.Data))
			m.Ack()
		})

		for i := 0; i < 10; i++ {
			r := topic.Publish(context.Background(), &pubsub.Message{Data: []byte("payload")})
			r.Get(context.Background())
		}

		topic.Stop()

		// some sleep to receive messages
		time.Sleep(5 * time.Second)

		// cleanup
		err = topic.Delete(ctx)
		assert.Nil(t, err)
		err = sub.Delete(ctx)
		assert.Nil(t, err)

		cancelFunc()
	}
}
