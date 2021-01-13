package messagebroker

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/razorpay/metro/pkg/messagebroker"
	"github.com/stretchr/testify/assert"
)

func Test_CreateValidTopic(t *testing.T) {

	ctx := context.Background()

	topic := fmt.Sprintf("dummytopic-%v", uuid.New())

	admin := getValidAdminClient()
	assert.NotNil(t, admin)

	resp, err := admin.CreateTopic(ctx, messagebroker.CreateTopicRequest{
		Name:          topic,
		NumPartitions: 1,
	})

	assert.Nil(t, err)
	assert.NotNil(t, resp)

	metadata, merr := admin.GetTopicMetadata(ctx, messagebroker.GetTopicMetadataRequest{Topic: topic, TimeoutMs: 6000})

	assert.Nil(t, merr)
	assert.NotNil(t, metadata)
}

func Test_CreateTopic_WrongPartitions(t *testing.T) {

	ctx := context.Background()

	topic := fmt.Sprintf("dummytopic-%v", uuid.New())

	admin := getValidAdminClient()
	assert.NotNil(t, admin)

	_, err := admin.CreateTopic(ctx, messagebroker.CreateTopicRequest{
		Name:          topic,
		NumPartitions: 0,
	})

	assert.NotNil(t, err)
}

func Test_CreateDuplicateTopic(t *testing.T) {

	ctx := context.Background()

	topic := fmt.Sprintf("dummytopic-%v", uuid.New())

	admin := getValidAdminClient()
	assert.NotNil(t, admin)

	resp, err := admin.CreateTopic(ctx, messagebroker.CreateTopicRequest{
		Name:          topic,
		NumPartitions: 1,
	})

	assert.Nil(t, err)
	assert.NotNil(t, resp)

	resp, duperr := admin.CreateTopic(ctx, messagebroker.CreateTopicRequest{
		Name:          topic,
		NumPartitions: 1,
	})

	assert.NotNil(t, duperr)
}

func Test_ProduceAndConsumeMessages(t *testing.T) {
	ctx := context.Background()

	topic := fmt.Sprintf("dummytopic-%v", uuid.New())

	producer, err := messagebroker.NewProducerClient(ctx, "kafka", getKafkaBrokerConfig(), &messagebroker.ProducerClientOptions{
		Topic:   topic,
		Timeout: 6000,
	})

	assert.Nil(t, err)
	assert.NotNil(t, producer)

	msgsToSend := 10

	// will assert whether all the same message_ids are consumed back or not
	var msgIds []string

	for i := 0; i < msgsToSend; i++ {
		newMsg := fmt.Sprintf("msg-%v", i)
		msgbytes, _ := json.Marshal(newMsg)
		msg := messagebroker.SendMessageToTopicRequest{
			Topic:   topic,
			Message: msgbytes,
			Timeout: 6000,
		}

		resp, rerr := producer.SendMessages(ctx, msg)
		assert.Nil(t, rerr)
		assert.NotNil(t, resp.MessageID)

		msgIds = append(msgIds, resp.MessageID)
	}

	// message produced count should match the number of message ids generated in response
	assert.Equal(t, msgsToSend, len(msgIds))

	// now consume the messages and assert the message ids generated in the previous step

}

func getValidAdminClient() messagebroker.Admin {
	admin, _ := messagebroker.NewAdminClient(context.Background(), "kafka", getKafkaBrokerConfig(), getAdminClientConfig())
	return admin
}

func getValidProducerClientConfig(topic string) *messagebroker.ProducerClientOptions {
	return &messagebroker.ProducerClientOptions{
		Topic:   topic,
		Timeout: 6000,
	}
}

func getValidConsumerClientConfig() *messagebroker.ConsumerClientOptions {
	return &messagebroker.ConsumerClientOptions{}
}
func getAdminClientConfig() *messagebroker.AdminClientOptions {
	return &messagebroker.AdminClientOptions{}
}

func getKafkaBrokerConfig() *messagebroker.BrokerConfig {
	return &messagebroker.BrokerConfig{
		Brokers: []string{"localhost:9092"},
	}
}
