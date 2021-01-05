// +build compatibility

package compatibility

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

var emulatorClient *pubsub.Client
var metroClient *pubsub.Client

func Test_Topic_CreateTopic(t *testing.T) {
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

func TestMain(m *testing.M) {
	var err error
	emulatorHost := fmt.Sprintf("%s:8085", os.Getenv("PUBSUB_TEST_HOST"))
	emulatorClient, err = pubsub.NewClient(context.Background(), "project-id",
		option.WithEndpoint(emulatorHost),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithInsecure()),
	)
	if err != nil {
		os.Exit(1)
	}
	metroHost := fmt.Sprintf("%s:8081", os.Getenv("METRO_TEST_HOST"))
	metroClient, err = pubsub.NewClient(context.Background(), "project-id",
		option.WithEndpoint(metroHost),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithInsecure()),
	)
	if err != nil {
		os.Exit(2)
	}
	exitVal := m.Run()
	os.Exit(exitVal)
}
