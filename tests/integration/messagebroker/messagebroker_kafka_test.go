package messagebroker

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/razorpay/metro/pkg/messagebroker"
	"github.com/stretchr/testify/assert"
)

func Test_CreateValidTopic_1(t *testing.T) {

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

func getValidAdminClient() messagebroker.Admin {
	admin, _ := messagebroker.NewAdminClient(context.Background(), "kafka", getKafkaBrokerConfig(), getAdminClientConfig())
	return admin
}

func getProducerClientConfig() *messagebroker.ProducerClientOptions {
	return &messagebroker.ProducerClientOptions{}
}
func getConsumerClientConfig() *messagebroker.ConsumerClientOptions {
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
