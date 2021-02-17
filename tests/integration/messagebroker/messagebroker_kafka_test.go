// +build integration

package messagebroker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/google/uuid"

	"github.com/razorpay/metro/pkg/messagebroker"
	"github.com/stretchr/testify/assert"
)

func Test_CreateValidTopic(t *testing.T) {

	topic := fmt.Sprintf("dummytopic-%v", uuid.New().String())

	admin, err := messagebroker.NewAdminClient(context.Background(), "kafka", getKafkaBrokerConfig(), getAdminClientConfig())
	assert.NotNil(t, admin)
	assert.Nil(t, err)

	resp, err := admin.CreateTopic(context.Background(), messagebroker.CreateTopicRequest{
		Name:          topic,
		NumPartitions: 1,
	})

	assert.Nil(t, err)
	assert.NotNil(t, resp)

	metadata, merr := admin.GetTopicMetadata(context.Background(), messagebroker.GetTopicMetadataRequest{Topic: topic, TimeoutSec: 2})

	assert.Nil(t, merr)
	assert.NotNil(t, metadata)
}

func Test_CreateTopic_WrongPartitions(t *testing.T) {

	ctx := context.Background()

	topic := fmt.Sprintf("dummytopic-%v", uuid.New())

	admin, err := messagebroker.NewAdminClient(context.Background(), "kafka", getKafkaBrokerConfig(), getAdminClientConfig())
	assert.NotNil(t, admin)
	assert.Nil(t, err)

	_, terr := admin.CreateTopic(ctx, messagebroker.CreateTopicRequest{
		Name:          topic,
		NumPartitions: 0,
	})

	assert.NotNil(t, terr)
}

func Test_CreateDuplicateTopic(t *testing.T) {

	ctx := context.Background()

	topic := fmt.Sprintf("dummytopic-%v", uuid.New())

	admin, err := messagebroker.NewAdminClient(context.Background(), "kafka", getKafkaBrokerConfig(), getAdminClientConfig())
	assert.NotNil(t, admin)
	assert.Nil(t, err)

	resp, err := admin.CreateTopic(ctx, messagebroker.CreateTopicRequest{
		Name:          topic,
		NumPartitions: 1,
	})

	assert.Nil(t, err)
	assert.NotNil(t, resp)

	// creating duplicate topic is noop and shouldn't return error
	resp, duperr := admin.CreateTopic(ctx, messagebroker.CreateTopicRequest{
		Name:          topic,
		NumPartitions: 1,
	})

	assert.Nil(t, duperr)
}

/*
Scenario being tested
1. Init Admin client
2. Create a new topic
3. Init a kafka producer.
4. Produce 5 messages to it.
5. Bring up a consumer client with consumer-group-1 and read from the aforementioned topic
6. Make sure 5 messages are received back
8. Make sure no new messages are available on the topic
*/
func Test_ProduceAndConsumeMessagesInDetail(t *testing.T) {
	topic := fmt.Sprintf("dummytopic-%v", uuid.New())

	// since auto-create is disable we need to create a topic on kafka via the admin client
	admin, err := messagebroker.NewAdminClient(context.Background(), "kafka", getKafkaBrokerConfig(), getAdminClientConfig())
	assert.NotNil(t, admin)
	assert.Nil(t, err)

	aresp, err := admin.CreateTopic(context.Background(), messagebroker.CreateTopicRequest{
		Name:          topic,
		NumPartitions: 1,
	})

	assert.Nil(t, err)
	assert.NotNil(t, aresp)

	// init a producer on the topic created
	producer, err := messagebroker.NewProducerClient(context.Background(), "kafka", getKafkaBrokerConfig(), &messagebroker.ProducerClientOptions{
		Topic:      topic,
		TimeoutSec: 2,
	})

	assert.Nil(t, err)
	assert.NotNil(t, producer)

	// the number of messages to be sent to the topic
	msgsToSend := 5

	// will assert whether all the same message_ids are consumed back or not
	var msgIds []string

	fmt.Printf("\nmsg sent to topic : %v", topic)
	for i := 0; i < msgsToSend; i++ {
		newMsg := fmt.Sprintf("msg-%v", i)
		msgbytes, _ := json.Marshal(newMsg)
		msg := messagebroker.SendMessageToTopicRequest{
			Topic:      topic,
			Message:    msgbytes,
			TimeoutSec: 2,
		}

		// send the message
		resp, rerr := producer.SendMessages(context.Background(), msg)
		assert.Nil(t, rerr)
		assert.NotNil(t, resp.MessageID)

		// store the message ids received. these will be used in the consume stage to validate
		msgIds = append(msgIds, resp.MessageID)
	}

	// now consume the messages and assert the message ids generated in the previous step
	consumer1, err := messagebroker.NewConsumerClient(context.Background(), "kafka", "id-1", getKafkaBrokerConfig(), &messagebroker.ConsumerClientOptions{
		Topic:   topic,
		GroupID: "dummy-group-1",
	})

	assert.Nil(t, err)
	assert.NotNil(t, consumer1)

	// first receive without commit
	resp, err := consumer1.ReceiveMessages(context.Background(), messagebroker.GetMessagesFromTopicRequest{
		NumOfMessages: int32(msgsToSend),
		TimeoutSec:    10,
	})

	assert.Nil(t, err)

	fmt.Printf("\n\nmsg received from topic : %v", topic)
	for offset, msg := range resp.OffsetWithMessages {
		fmt.Printf("\noffset [%v], message [%v]", offset, msg)
	}

	// message produced count should match the number of message ids generated in response
	assert.Equal(t, msgsToSend, len(resp.OffsetWithMessages))

	// spwan a new consumer and try to re-receive after commit and make sure no new messages are available
	consumer3, err := messagebroker.NewConsumerClient(context.Background(), "kafka", "id-2", getKafkaBrokerConfig(), &messagebroker.ConsumerClientOptions{
		Topic:   topic,
		GroupID: "dummy-group-1",
	})

	assert.Nil(t, err)
	assert.NotNil(t, consumer3)

	resp3, rerr := consumer3.ReceiveMessages(context.Background(), messagebroker.GetMessagesFromTopicRequest{
		NumOfMessages: int32(msgsToSend),
		TimeoutSec:    2,
	})

	assert.NotNil(t, resp3)
	assert.Equal(t, len(resp3.OffsetWithMessages), 0)
	assert.Nil(t, rerr)
}

func getAdminClientConfig() *messagebroker.AdminClientOptions {
	return &messagebroker.AdminClientOptions{}
}

func getKafkaBrokerConfig() *messagebroker.BrokerConfig {
	kafKaBroker := fmt.Sprintf("%v:9092", os.Getenv("KAFKA_TEST_HOST"))
	fmt.Println("using kafKaBroker", kafKaBroker)
	return &messagebroker.BrokerConfig{
		Brokers: []string{kafKaBroker},
		Consumer: &messagebroker.ConsumerConfig{
			OffsetReset:      "latest",
			EnableAutoCommit: false,
		},
	}
}
