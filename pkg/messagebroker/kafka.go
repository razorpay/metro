package messagebroker

import (
	"context"
	"fmt"
	"strings"
	"time"

	kakfapkg "github.com/confluentinc/confluent-kafka-go/kafka"
)

// KafkaBroker for kafka
type KafkaBroker struct {
	Producer *kakfapkg.Producer
	Consumer *kakfapkg.Consumer
	Admin    *kakfapkg.AdminClient
	Ctx      context.Context
	Config   *BrokerConfig
}

// NewKafkaConsumer returns a kafka consumer
func NewKafkaConsumer(ctx context.Context, bConfig *BrokerConfig) (Consumer, error) {
	c, err := kakfapkg.NewConsumer(&kakfapkg.ConfigMap{
		"bootstrap.servers":  bConfig.Brokers,
		"group.id":           bConfig.Consumer.GroupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	})

	if err != nil {
		return nil, err
	}

	c.SubscribeTopics([]string{bConfig.Consumer.Topic}, nil)

	return &KafkaBroker{
		Consumer: c,
		Ctx:      ctx,
		Config:   bConfig,
	}, nil
}

// NewKafkaProducer returns a kafka producer
func NewKafkaProducer(ctx context.Context, bConfig *BrokerConfig) (Producer, error) {
	p, err := kakfapkg.NewProducer(&kakfapkg.ConfigMap{"bootstrap.servers": strings.Join(bConfig.Brokers, ",")})
	if err != nil {
		return nil, err
	}

	return &KafkaBroker{
		Producer: p,
		Ctx:      ctx,
		Config:   bConfig,
	}, nil
}

// NewKafkaAdmin returns a kafka admin
func NewKafkaAdmin(ctx context.Context, bConfig *BrokerConfig) (Admin, error) {
	a, err := kakfapkg.NewAdminClient(&kakfapkg.ConfigMap{})
	if err != nil {
		return nil, err
	}

	return &KafkaBroker{
		Admin:  a,
		Ctx:    ctx,
		Config: bConfig,
	}, nil
}

func (k *KafkaBroker) CreateTopic(ctx context.Context, request CreateTopicRequest) (CreateTopicResponse, error) {

	topics := make([]kakfapkg.TopicSpecification, 0)
	ts := kakfapkg.TopicSpecification{
		Topic:         request.Name,
		NumPartitions: request.NumPartitions,
	}
	topics = append(topics, ts)
	_, err := k.Admin.CreateTopics(k.Ctx, topics, nil)

	return CreateTopicResponse{}, err
}

func (k *KafkaBroker) DeleteTopic(ctx context.Context, request DeleteTopicRequest) (DeleteTopicResponse, error) {

	topics := make([]string, 0)
	topics = append(topics, request.Name)
	_, err := k.Admin.DeleteTopics(k.Ctx, topics)
	return DeleteTopicResponse{}, err
}

func (k *KafkaBroker) SendMessage(ctx context.Context, request SendMessageToTopicRequest) (SendMessageToTopicResponse, error) {

	deliveryChan := make(chan kakfapkg.Event)

	var kHeaders []kakfapkg.Header
	if request.Attributes != nil {
		for _, attribute := range request.Attributes {
			for k, v := range attribute {
				kHeaders = append(kHeaders, kakfapkg.Header{
					Key:   k,
					Value: v,
				})
			}
		}
	}

	err := k.Producer.Produce(&kakfapkg.Message{
		TopicPartition: kakfapkg.TopicPartition{Topic: &request.Topic, Partition: kakfapkg.PartitionAny},
		Value:          request.Message,
		Headers:        kHeaders,
	}, deliveryChan)

	e := <-deliveryChan
	m := e.(*kakfapkg.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}

	close(deliveryChan)

	return SendMessageToTopicResponse{MessageId: m.String()}, err
}

func (k *KafkaBroker) GetMessages(ctx context.Context, request GetMessagesFromTopicRequest) (GetMessagesFromTopicResponse, error) {

	var msgs []string
	for {
		var interval time.Duration
		interval = k.Config.Consumer.PollInterval
		if len(msgs) == 0 {
			interval = request.Timeout
		}

		msg, err := k.Consumer.ReadMessage(interval)
		if err == nil {
			msgs = append(msgs, string(msg.Value))
			if len(msgs) == request.NumOfMessages {
				k.Consumer.Commit()
				return GetMessagesFromTopicResponse{Messages: msgs}, nil
			}

		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			return GetMessagesFromTopicResponse{}, err
		}
	}
	return GetMessagesFromTopicResponse{Messages: msgs}, nil
}

// Commit commits the offset
func (k *KafkaBroker) Commit(ctx context.Context) (CommitOnTopicResponse, error) {
	_, err := k.Consumer.Commit()
	return CommitOnTopicResponse{}, err
}
