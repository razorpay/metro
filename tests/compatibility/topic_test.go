// +build compatibility

package compatibility

/*
import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

func Test_Topic_CreateTopic(t *testing.T) {
	client, err := pubsub.NewClient(context.Background(), "project-id",
		option.WithEndpoint("localhost:8085"),
		option.WithoutAuthentication(),
		//option.WithHTTPClient(http.DefaultClient),
		option.WithGRPCDialOption(grpc.WithInsecure()),
	)

	assert.Nil(t, err)
	topic, err := client.CreateTopic(context.Background(), "topic-nam4")
	fmt.Println(err)
	assert.Nil(t, err)
	assert.NotNil(t, topic)
	r := topic.Publish(context.Background(), &pubsub.Message{Data: []byte("payload")})
	r.Get(context.Background())
	topic.Stop()
	sub, err := client.CreateSubscription(context.Background(), "sub-name1",
		pubsub.SubscriptionConfig{Topic: topic})
	fmt.Println(err)
	assert.Nil(t,err)
	sub.ReceiveSettings.Synchronous = true
	ctx, cancelFunc := context.WithCancel(context.Background())
	fmt.Printf("here")

	go sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		//m.Ack()
	})
	time.Sleep(20 * time.Millisecond)
	cancelFunc()
	if err != nil {
		// Handle error.
	}
}
*/
