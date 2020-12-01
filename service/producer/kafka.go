package producer

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/razorpay/metro/internal/config"
	kafkabroker "github.com/razorpay/metro/pkg/messagebroker/kafka"
)

type KafkaProducer struct {
	producer *kafka.Producer
}

func newKakfaProducer(config *config.ConnectionParams) IProducer {
	bConfig := &kafkabroker.BrokerConfig{
		Producer: kafkabroker.ProducerConfig{
			Brokers: config.Brokers,
		},
	}
	broker, err := kafkabroker.NewKafkaBroker(nil, bConfig)
	if err != nil {
		panic(err)
	}

	if err != nil {
		panic(err)
	}

	return &KafkaProducer{producer: broker.Producer}
}

func (k *KafkaProducer) PublishMessage(messages *Message) (string, error) {

	deliveryChan := make(chan kafka.Event)

	topic := messages.topic
	kmsg := k.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          messages.message,
	}, deliveryChan)

	event := <-deliveryChan
	m := event.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}

	close(deliveryChan)

	return m.String(), kmsg
}
