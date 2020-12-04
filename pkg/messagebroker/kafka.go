package messagebroker

import (
	"context"
	"fmt"
	"strings"
	"time"

	kakfapkg "github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaBroker struct {
	Producer *kakfapkg.Producer
	Consumer *kakfapkg.Consumer
	Admin    *kakfapkg.AdminClient
	Ctx      context.Context
	Config   *BrokerConfig
}

func NewKafkaBroker(ctx context.Context, bConfig *BrokerConfig) (Broker, error) {

	// init producer
	producer, err := kakfapkg.NewProducer(&kakfapkg.ConfigMap{"bootstrap.servers": strings.Join(bConfig.Producer.Brokers, ",")})
	if err != nil {
		return nil, err
	}

	// init consumer
	consumer, err := kakfapkg.NewConsumer(&kakfapkg.ConfigMap{
		"bootstrap.servers":  bConfig.Consumer.BrokerList,
		"group.id":           bConfig.Consumer.GroupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	})

	if err != nil {
		return nil, err
	}

	consumer.SubscribeTopics([]string{bConfig.Consumer.Topic}, nil)

	// init admin
	admin, err := kakfapkg.NewAdminClient(&kakfapkg.ConfigMap{})
	if err != nil {
		return nil, err
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

func (k KafkaBroker) CreateTopic(topic string) error {
	//TODO: Implement me
	return nil
}

func (k KafkaBroker) DeleteTopic(topic string) error {
	//TODO: Implement me
	return nil
}

func (k KafkaBroker) Produce(topic string, message []byte) (string, error) {
	//TODO: Implement me
	return "", nil
}

func (k KafkaBroker) GetMessages(numOfMessages int, timeout time.Duration) ([]string, error) {
	var msgs []string
	for {
		var interval time.Duration
		interval = k.Config.Consumer.PollInterval
		if len(msgs) == 0 {
			interval = timeout
		}

		msg, err := k.Consumer.ReadMessage(interval)
		if err == nil {
			msgs = append(msgs, string(msg.Value))
			if len(msgs) == numOfMessages {
				k.Consumer.Commit()
				return msgs, nil
			}

		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)

			return msgs, nil
		}
	}
}

func (k KafkaBroker) Commit() {
	k.Consumer.Commit()
}
