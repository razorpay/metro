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

func Test_Pubsub1(t *testing.T) {
	//t.SkipNow()
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
		if err != nil {
			t.Logf("error in create subscription: %s", err)
		}

		assert.Nil(t, err)

		ctx, cancelFunc := context.WithCancel(context.Background())

		numOfMessages := 10

		//sub1.ReceiveSettings.Synchronous = true
		sub.ReceiveSettings.NumGoroutines = 1
		//sub.ReceiveSettings.MaxExtension = 10 * time.Minute
		//sub.ReceiveSettings.MaxExtensionPeriod = 10 * time.Minute
		//sub.ReceiveSettings.MaxOutstandingBytes = 0
		go sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			index++
			da := 0
			if m.DeliveryAttempt != nil {
				da = *m.DeliveryAttempt
			}

			t.Logf("[%d] Got message: id=[%v], deliveryAttempt=[%v], data=[%v]", index, m.ID, da, string(m.Data))
			m.Ack()
		})

		//topic.EnableMessageOrdering = true
		for i := 0; i < numOfMessages; i++ {
			r := topic.Publish(context.Background(), &pubsub.Message{Data: []byte(fmt.Sprintf("payload %d", i))})
			r.Get(context.Background())
		}

		time.Sleep(time.Duration(numOfMessages) * time.Second)
		topic.Stop()

		// cleanup
		err = topic.Delete(ctx)
		assert.Nil(t, err)
		err = sub.Delete(ctx)
		assert.Nil(t, err)

		cancelFunc()
	}
}

// More TEST scenarios:
// Nack : see if it goes to retry
// consumer group
// no of goroutines : 2
// make one stream, produce messages on topic with 2 partitions. Then start another stream : topic partition should get rebalanced b/w the two
func Test_Pubsub2(t *testing.T) {
	//t.SkipNow()
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
		if err != nil {
			t.Logf("error in create subscription: %s", err)
		}

		assert.Nil(t, err)

		ctx, cancelFunc := context.WithCancel(context.Background())

		numOfMessages := 1

		isFirst := true

		//sub1.ReceiveSettings.Synchronous = true
		sub.ReceiveSettings.NumGoroutines = 1
		//sub.ReceiveSettings.MaxExtension = 10 * time.Minute
		//sub.ReceiveSettings.MaxExtensionPeriod = 10 * time.Minute
		//sub.ReceiveSettings.MaxOutstandingBytes = 0
		go sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			index++
			da := 0
			if m.DeliveryAttempt != nil {
				da = *m.DeliveryAttempt
			}

			if isFirst {
				isFirst = false
				t.Logf("NAcking [%d] Got message: id=[%v], deliveryAttempt=[%v], data=[%v]", index, m.ID, da, string(m.Data))
				m.Nack()
			} else {
				t.Logf("Acking [%d] Got message: id=[%v], deliveryAttempt=[%v], data=[%v]", index, m.ID, da, string(m.Data))
				m.Ack()
			}
		})

		//topic.EnableMessageOrdering = true
		for i := 0; i < numOfMessages; i++ {
			r := topic.Publish(context.Background(), &pubsub.Message{Data: []byte(fmt.Sprintf("payload %d", i))})
			r.Get(context.Background())
		}

		time.Sleep(time.Duration(10) * time.Second)
		topic.Stop()

		// cleanup
		err = topic.Delete(ctx)
		assert.Nil(t, err)
		err = sub.Delete(ctx)
		assert.Nil(t, err)

		cancelFunc()
	}
}
