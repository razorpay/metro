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

// NewKafkaBroker returns a kafka broker
func NewKafkaBroker(ctx context.Context, bConfig *BrokerConfig) (Broker, error) {

	var producer *kakfapkg.Producer
	if bConfig.Producer != nil {
		// init producer
		p, err := kakfapkg.NewProducer(&kakfapkg.ConfigMap{"bootstrap.servers": strings.Join(bConfig.Producer.Brokers, ",")})
		if err != nil {
			return nil, err
		}
		producer = p
	}

	var consumer *kakfapkg.Consumer
	if bConfig.Consumer != nil {
		// init consumer
		c, err := kakfapkg.NewConsumer(&kakfapkg.ConfigMap{
			"bootstrap.servers":  bConfig.Consumer.BrokerList,
			"group.id":           bConfig.Consumer.GroupID,
			"auto.offset.reset":  "earliest",
			"enable.auto.commit": "false",
		})

		if err != nil {
			return nil, err
		}

		c.SubscribeTopics([]string{bConfig.Consumer.Topic}, nil)

		consumer = c
	}

	var admin *kakfapkg.AdminClient
	if bConfig.Admin != nil {
		// init admin
		a, err := kakfapkg.NewAdminClient(&kakfapkg.ConfigMap{})
		if err != nil {
			return nil, err
		}
		admin = a
	}

	if ctx == nil {
		ctx = context.Background()
	}

	return &KafkaBroker{
		Consumer: consumer,
		Producer: producer,
		Admin:    admin,
		Ctx:      ctx,
		Config:   bConfig,
	}, nil
}

func (k *KafkaBroker) CreateTopic(request CreateTopicRequest) CreateTopicResponse {
	// TODO : validate request
	topics := make([]kakfapkg.TopicSpecification, 0)
	ts := kakfapkg.TopicSpecification{
		Topic:         request.Name,
		NumPartitions: request.NumPartitions,
	}
	topics = append(topics, ts)
	result, err := k.Admin.CreateTopics(k.Ctx, topics, nil)
	return CreateTopicResponse{
		BaseResponse{
			Error:    err,
			Response: result,
		},
	}
}

func (k *KafkaBroker) DeleteTopic(request DeleteTopicRequest) DeleteTopicResponse {
	// TODO : validate request
	topics := make([]string, 0)
	topics = append(topics, request.Name)
	result, err := k.Admin.DeleteTopics(k.Ctx, topics)
	return DeleteTopicResponse{
		BaseResponse{
			Error:    err,
			Response: result,
		},
	}
}

func (k *KafkaBroker) SendMessage(request SendMessageToTopicRequest) SendMessageToTopicResponse {
	// TODO : validate request

	deliveryChan := make(chan kakfapkg.Event)

	err := k.Producer.Produce(&kakfapkg.Message{
		TopicPartition: kakfapkg.TopicPartition{Topic: &request.Topic, Partition: kakfapkg.PartitionAny},
		Value:          request.Message,
		Headers:        []kakfapkg.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
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

	return SendMessageToTopicResponse{
		BaseResponse: BaseResponse{
			Error:    err,
			Response: m,
		},
		MessageId: e.String(),
	}
}

func (k *KafkaBroker) GetMessages(request GetMessagesFromTopicRequest) GetMessagesFromTopicResponse {
	// TODO : validate request
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
				return GetMessagesFromTopicResponse{
					Messages: msgs,
				}
			}

		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)

			return GetMessagesFromTopicResponse{
				Messages: msgs,
				BaseResponse: BaseResponse{
					Error: err,
				},
			}
		}
	}
}

// Commit commits the offset
func (k *KafkaBroker) Commit() {
	k.Consumer.Commit()
}
