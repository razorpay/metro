// +build compatibility

package compatibility

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_Pubsub1(t *testing.T) {
	t.SkipNow()
	topicName := fmt.Sprintf("topic-%s", uuid.New().String()[0:4])
	subscription := fmt.Sprintf("sub-%s", uuid.New().String()[0:4])

	// This is just a placeholder test.
	// TODO: fix it with more tests and better structure
	index := 0
	for _, client := range []*pubsub.Client{metroClient, emulatorClient} {
		topic, err := client.CreateTopic(context.Background(), topicName)
		assert.Nil(t, err)
		assert.NotNil(t, topic)

		sub, err := client.CreateSubscription(context.Background(), subscription,
			pubsub.SubscriptionConfig{Topic: topic})
		assert.Nil(t, err)

		ctx, cancelFunc := context.WithCancel(context.Background())

		var (
			wg sync.WaitGroup

			numOfMsgs = 10
		)
		wg.Add(numOfMsgs)

		//sub1.ReceiveSettings.Synchronous = true
		sub.ReceiveSettings.NumGoroutines = 1
		go sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			index++
			da := 0
			if m.DeliveryAttempt != nil {
				da = *m.DeliveryAttempt
			}

			t.Logf("[%d] Got message: id=[%v], deliveryAttempt=[%v], data=[%v]", index, m.ID, da, string(m.Data))
			m.Ack()

			wg.Done()
		})

		//topic.EnableMessageOrdering = true
		for i := 0; i < numOfMsgs; i++ {
			r := topic.Publish(context.Background(), &pubsub.Message{Data: []byte(fmt.Sprintf("payload %d", i))})
			r.Get(context.Background())
		}

		topic.Stop()

		wg.Wait()

		// cleanup
		err = topic.Delete(ctx)
		assert.Nil(t, err)
		err = sub.Delete(ctx)
		assert.Nil(t, err)

		cancelFunc()
	}
}

func Test_Pubsub2(t *testing.T) {
	topicName := fmt.Sprintf("topic-%s", uuid.New().String()[0:4])
	subscription := fmt.Sprintf("sub-%s", uuid.New().String()[0:4])

	// This is just a placeholder test.
	// TODO: fix it with more tests and better structure
	index := 0
	for _, client := range []*pubsub.Client{metroClient, emulatorClient} {
		topic, err := client.CreateTopic(context.Background(), topicName)
		assert.Nil(t, err)
		assert.NotNil(t, topic)

		sub, err := client.CreateSubscription(context.Background(), subscription,
			pubsub.SubscriptionConfig{Topic: topic})
		assert.Nil(t, err)

		ctx, cancelFunc := context.WithCancel(context.Background())

		var (
			wg sync.WaitGroup

			numOfMsgs = 10
		)
		wg.Add(numOfMsgs)

		//sub1.ReceiveSettings.Synchronous = true
		sub.ReceiveSettings.NumGoroutines = 1
		go sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			index++
			da := 0
			if m.DeliveryAttempt != nil {
				da = *m.DeliveryAttempt
			}

			if index%2 == 0 {
				// ack every alternate message
				m.Ack()
				t.Logf("Ack'd ==> [%d] Got message: id=[%v], deliveryAttempt=[%v], data=[%v]", index, m.ID, da, string(m.Data))

			} else {
				t.Logf("Deadline evicted ==> [%d] Got message: id=[%v], deliveryAttempt=[%v], data=[%v]", index, m.ID, da, string(m.Data))
				//m.Nack()
			}
			wg.Done()
		})

		//topic.EnableMessageOrdering = true
		for i := 0; i < numOfMsgs; i++ {
			r := topic.Publish(context.Background(), &pubsub.Message{Data: []byte(fmt.Sprintf("payload %d", i))})
			r.Get(context.Background())
		}

		topic.Stop()

		wg.Wait()

		// cleanup
		err = topic.Delete(ctx)
		assert.Nil(t, err)
		err = sub.Delete(ctx)
		assert.Nil(t, err)

		cancelFunc()
	}
}
