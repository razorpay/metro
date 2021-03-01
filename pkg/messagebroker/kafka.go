package messagebroker

import (
	"context"
	"fmt"
	"strings"
	"time"

	kafkapkg "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/rs/xid"
)

const (
	messageID = "messageID"
)

// KafkaBroker for kafka
type KafkaBroker struct {
	Producer *kafkapkg.Producer
	Consumer *kafkapkg.Consumer
	Admin    *kafkapkg.AdminClient
	Ctx      context.Context

	// holds the broker config
	Config *BrokerConfig

	// holds the client configs
	POptions *ProducerClientOptions
	COptions *ConsumerClientOptions
	AOptions *AdminClientOptions
}

// newKafkaConsumerClient returns a kafka consumer
func newKafkaConsumerClient(ctx context.Context, bConfig *BrokerConfig, id string, options *ConsumerClientOptions) (Consumer, error) {
	err := validateKafkaConsumerBrokerConfig(bConfig)
	if err != nil {
		return nil, err
	}

	err = validateKafkaConsumerClientConfig(options)
	if err != nil {
		return nil, err
	}

	configMap := &kafkapkg.ConfigMap{
		"bootstrap.servers":  strings.Join(bConfig.Brokers, ","),
		"group.id":           options.GroupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
		"group.instance.id":  id,
	}

	if bConfig.EnableTLS {
		certs, err := readKafkaCerts(bConfig.CertDir)
		if err != nil {
			return nil, err
		}

		// Refer : https://github.com/edenhill/librdkafka/wiki/Using-SSL-with-librdkafka#configure-librdkafka-client
		configMap.SetKey("security.protocol", "ssl")
		configMap.SetKey("ssl.ca.location", certs.caCertPath)
		configMap.SetKey("ssl.certificate.location", certs.userCertPath)
		configMap.SetKey("ssl.key.location", certs.userKeyPath)
	}

	c, err := kafkapkg.NewConsumer(configMap)
	if err != nil {
		return nil, err
	}

	c.SubscribeTopics([]string{options.Topic}, nil)

	return &KafkaBroker{
		Consumer: c,
		Config:   bConfig,
		COptions: options,
	}, nil
}

// newKafkaProducerClient returns a kafka producer
func newKafkaProducerClient(ctx context.Context, bConfig *BrokerConfig, options *ProducerClientOptions) (Producer, error) {
	err := validateKafkaProducerBrokerConfig(bConfig)
	if err != nil {
		return nil, err
	}

	err = validateKafkaProducerClientConfig(options)
	if err != nil {
		return nil, err
	}

	configMap := &kafkapkg.ConfigMap{
		"bootstrap.servers": strings.Join(bConfig.Brokers, ","),
	}

	if bConfig.EnableTLS {
		certs, err := readKafkaCerts(bConfig.CertDir)
		if err != nil {
			return nil, err
		}

		configMap.SetKey("security.protocol", "ssl")
		configMap.SetKey("ssl.ca.location", certs.caCertPath)
		configMap.SetKey("ssl.certificate.location", certs.userCertPath)
		configMap.SetKey("ssl.key.location", certs.userKeyPath)
	}

	p, err := kafkapkg.NewProducer(configMap)
	if err != nil {
		return nil, err
	}

	return &KafkaBroker{
		Producer: p,
		Config:   bConfig,
		POptions: options,
	}, nil
}

// newKafkaAdminClient returns a kafka admin
func newKafkaAdminClient(ctx context.Context, bConfig *BrokerConfig, options *AdminClientOptions) (Admin, error) {
	err := validateKafkaAdminBrokerConfig(bConfig)
	if err != nil {
		return nil, err
	}

	err = validateKafkaAdminClientConfig(options)
	if err != nil {
		return nil, err
	}

	configMap := &kafkapkg.ConfigMap{
		"bootstrap.servers": strings.Join(bConfig.Brokers, ","),
	}

	if bConfig.EnableTLS {
		certs, err := readKafkaCerts(bConfig.CertDir)
		if err != nil {
			return nil, err
		}

		configMap.SetKey("security.protocol", "ssl")
		configMap.SetKey("ssl.ca.location", certs.caCertPath)
		configMap.SetKey("ssl.certificate.location", certs.userCertPath)
		configMap.SetKey("ssl.key.location", certs.userKeyPath)
	}

	a, err := kafkapkg.NewAdminClient(configMap)

	if err != nil {
		return nil, err
	}

	return &KafkaBroker{
		Admin:    a,
		Config:   bConfig,
		AOptions: options,
	}, nil
}

type kafkaCerts struct {
	caCertPath   string
	userCertPath string
	userKeyPath  string
}

func readKafkaCerts(certDir string) (*kafkaCerts, error) {
	caCertPath, err := getCertFile(certDir, "ca-cert.pem")
	userCertPath, err := getCertFile(certDir, "user-cert.pem")
	userKeyPath, err := getCertFile(certDir, "user.key")

	if err != nil {
		return nil, err
	}

	return &kafkaCerts{
		caCertPath:   caCertPath,
		userCertPath: userCertPath,
		userKeyPath:  userKeyPath,
	}, nil
}

// CreateTopic creates a new topic if not available
func (k *KafkaBroker) CreateTopic(ctx context.Context, request CreateTopicRequest) (CreateTopicResponse, error) {

	tp := normalizeTopicName(request.Name)
	logger.Ctx(ctx).Infow("received request to create kafka topic", "request", request, "normalizedTopicName", tp)

	topics := make([]kafkapkg.TopicSpecification, 0)
	ts := kafkapkg.TopicSpecification{
		Topic:             tp,
		NumPartitions:     request.NumPartitions,
		ReplicationFactor: 1,
	}
	topics = append(topics, ts)
	topicsResp, err := k.Admin.CreateTopics(ctx, topics, kafkapkg.SetAdminOperationTimeout(59*time.Second))
	if err != nil {
		return CreateTopicResponse{
			Response: topicsResp,
		}, err
	}

	for _, tp := range topicsResp {
		if tp.Error.Code() != kafkapkg.ErrNoError && tp.Error.Code() != kafkapkg.ErrTopicAlreadyExists {
			return CreateTopicResponse{
				Response: topicsResp,
			}, fmt.Errorf("kafka: %v", tp.Error.String())
		}
	}

	logger.Ctx(ctx).Infow("kafka topic creation successfully completed", "response", topicsResp)

	return CreateTopicResponse{
		Response: topicsResp,
	}, nil
}

// DeleteTopic deletes an existing topic
func (k *KafkaBroker) DeleteTopic(ctx context.Context, request DeleteTopicRequest) (DeleteTopicResponse, error) {

	topics := make([]string, 0)
	topics = append(topics, request.Name)
	resp, err := k.Admin.DeleteTopics(ctx, topics)
	return DeleteTopicResponse{
		Response: resp,
	}, err
}

// GetTopicMetadata fetches the given topics metadata stored in the broker
func (k *KafkaBroker) GetTopicMetadata(ctx context.Context, req GetTopicMetadataRequest) (GetTopicMetadataResponse, error) {
	metadata, err := k.Admin.GetMetadata(&req.Topic, false, req.TimeoutSec*1000)
	if err != nil {
		return GetTopicMetadataResponse{}, err
	}

	return GetTopicMetadataResponse{
		Response: map[string]interface{}{
			"brokers":           metadata.Brokers,
			"originatingBroker": metadata.Brokers,
			"topics":            metadata.Topics,
		},
	}, err
}

// SendMessage sends a message on the topic
func (k *KafkaBroker) SendMessage(ctx context.Context, request SendMessageToTopicRequest) (*SendMessageToTopicResponse, error) {

	var kHeaders []kafkapkg.Header
	if request.Attributes != nil {
		for _, attribute := range request.Attributes {
			for k, v := range attribute {
				kHeaders = append(kHeaders, kafkapkg.Header{
					Key:   k,
					Value: v,
				})
			}
		}
	}

	// generate a message id and attach
	msgID := xid.New().String()
	kHeaders = append(kHeaders, kafkapkg.Header{
		Key:   messageID,
		Value: []byte(msgID),
	})

	deliveryChan := make(chan kafkapkg.Event)

	tp := normalizeTopicName(request.Topic)
	err := k.Producer.Produce(&kafkapkg.Message{
		TopicPartition: kafkapkg.TopicPartition{Topic: &tp, Partition: kafkapkg.PartitionAny},
		Value:          request.Message,
		Key:            []byte(request.OrderingKey),
		Headers:        kHeaders,
	}, deliveryChan)
	if err != nil {
		return nil, err
	}

	var m *kafkapkg.Message
	select {
	case event := <-deliveryChan:
		m = event.(*kafkapkg.Message)
	case <-time.After(time.Duration(request.TimeoutSec) * time.Second):
		return nil, fmt.Errorf("failed to produce message to topic [%v] due to timeout [%v]", &request.Topic, k.POptions.TimeoutSec)
	}

	if m != nil && m.TopicPartition.Error != nil {
		return nil, m.TopicPartition.Error
	}

	close(deliveryChan)

	return &SendMessageToTopicResponse{MessageID: msgID}, nil
}

//ReceiveMessages gets tries to get the number of messages mentioned in the param "numOfMessages"
//from the previous committed offset. If the available messages in the queue are less, returns
// how many ever messages are available
func (k *KafkaBroker) ReceiveMessages(ctx context.Context, request GetMessagesFromTopicRequest) (*GetMessagesFromTopicResponse, error) {
	msgs := make(map[string]ReceivedMessage, 0)
	for {
		var msgID string
		msg, err := k.Consumer.ReadMessage(time.Duration(request.TimeoutSec) * time.Second)
		if err == nil {
			for _, v := range msg.Headers {
				if v.Key == messageID {
					msgID = string(v.Value)
				}
			}
			msgs[fmt.Sprintf("%v", int64(msg.TopicPartition.Offset))] = ReceivedMessage{msg.Value, msgID, msg.TopicPartition.Partition, int32(msg.TopicPartition.Offset), msg.Timestamp}
			if int32(len(msgs)) == request.NumOfMessages {
				return &GetMessagesFromTopicResponse{OffsetWithMessages: msgs}, nil
			}
		} else if err.(kafkapkg.Error).Code() == kafkapkg.ErrTimedOut {
			return &GetMessagesFromTopicResponse{OffsetWithMessages: msgs}, nil
		} else {
			// The client will automatically try to recover from all errors.
			logger.Ctx(ctx).Errorw("error in receiving messages", "msg", err.Error())
			return nil, err
		}
	}
	return &GetMessagesFromTopicResponse{OffsetWithMessages: msgs}, nil
}

// CommitByPartitionAndOffset Commits messages if any
//This func will commit the message consumed
//by all the previous calls to GetMessages
func (k *KafkaBroker) CommitByPartitionAndOffset(ctx context.Context, request CommitOnTopicRequest) (CommitOnTopicResponse, error) {
	tp := kafkapkg.TopicPartition{
		Topic: &request.Topic,
		//Partition: request.Partition,
		Offset: kafkapkg.Offset(request.Offset),
	}

	tps := make([]kafkapkg.TopicPartition, 0)
	tps = append(tps, tp)

	resp, err := k.Consumer.CommitOffsets(tps)
	return CommitOnTopicResponse{
		Response: resp,
	}, err
}

// CommitByMsgID Commits a message by ID
func (k *KafkaBroker) CommitByMsgID(_ context.Context, _ CommitOnTopicRequest) (CommitOnTopicResponse, error) {
	// unused for kafka
	return CommitOnTopicResponse{}, nil
}
