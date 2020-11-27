package kafka

import (
	"fmt"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const (
	POLL_INTERVAL = 1
)

type Consumer struct {
	consumer           *kafka.Consumer
	UncommitedMessages []string
}

type ConsumerConfig struct {
	BrokerList string
	GroupID    string
	Topic      string
}

func NewConsumer(config interface{}) *Consumer {
	consumer := &Consumer{}

	consumer.Intialise(config.(ConsumerConfig))

	return consumer
}

func (consumer *Consumer) Intialise(config KafkaConsumerConfig) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  config.BrokerList,
		"group.id":           config.GroupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{config.Topic}, nil)
	consumer.consumer = c
	fmt.Println("Initialied")
}
func (consumer *Consumer) close() {
	consumer.consumer.Close()
}

//GetMessages returns the required number of messages
//if there are those many messages in the queue or returns
//how many ever are available - (waits for the time POLLING_INTERVAL before returning)
// if there are no messages in the queue - it waits indefinitely for the messages to
//arrive
func (consumer *Consumer) GetMessages(numOfMessages int, timeout time.Duration) ([]string, error) {
	if len(consumer.UncommitedMessages) > 0 {
		return nil, nil
	}
	for {
		c := consumer.consumer
		var interval time.Duration
		interval = POLL_INTERVAL
		if len(consumer.UncommitedMessages) == 0 {
			interval = timeout
		}

		msg, err := c.ReadMessage(interval)
		if err == nil {
			//fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			consumer.UncommitedMessages = append(consumer.UncommitedMessages, string(msg.Value))
			if len(consumer.UncommitedMessages) == numOfMessages {
				c.Commit()
				return consumer.UncommitedMessages, nil
			}

		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)

			return consumer.UncommitedMessages, nil
		}
	}

}

//Gets uncommitted messages if any
func (consumer *Consumer) GetUnCommitedMessages(numOfMessages int) []string {
	return consumer.UncommitedMessages
}

func (consumer *Consumer) Commit() {
	consumer.consumer.Commit()
}
