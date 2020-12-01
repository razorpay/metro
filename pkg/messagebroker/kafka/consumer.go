package kafka

import (
	"fmt"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Consumer struct {
	consumer *kafka.Consumer
	config   ConsumerConfig
}

type ConsumerConfig struct {
	BrokerList   string
	GroupID      string
	Topic        string
	PollInterval time.Duration
}

func NewConsumer(config interface{}) *Consumer {
	consumer := &Consumer{}

	consumer.intialise(config.(ConsumerConfig))

	return consumer
}

func (consumer *Consumer) intialise(config ConsumerConfig) error{
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  config.BrokerList,
		"group.id":           config.GroupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	})

	if err != nil {
		return err
	}

	c.SubscribeTopics([]string{config.Topic}, nil)
	consumer.consumer = c

	return nil
}
func (consumer *Consumer) close() {
	consumer.consumer.Close()
}

func (consumer *Consumer) GetMessages(numOfMessages int, timeout time.Duration) ([]string, error) {
	var msgs []string
	for {
		c := consumer.consumer
		var interval time.Duration
		interval = consumer.config.PollInterval
		if len(msgs) == 0 {
			interval = timeout
		}

		msg, err := c.ReadMessage(interval)
		if err == nil {
			msgs = append(msgs, string(msg.Value))
			if len(msgs) == numOfMessages {
				c.Commit()
				return msgs, nil
			}

		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)

			return msgs, nil
		}
	}

}

func (consumer *Consumer) Commit() {
	consumer.consumer.Commit()
}
