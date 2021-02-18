package messagebroker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/razorpay/metro/pkg/logger"

	kakfapkg "github.com/confluentinc/confluent-kafka-go/kafka"
)

// KafkaBroker for kafka
type KafkaBroker struct {
	Producer *kakfapkg.Producer
	Consumer *kakfapkg.Consumer
	Admin    *kakfapkg.AdminClient

	// holds the broker config
	Config *BrokerConfig

	// holds the client configs
	POptions *ProducerClientOptions
	COptions *ConsumerClientOptions
	AOptions *AdminClientOptions
}

// newKafkaConsumerClient returns a kafka consumer
func newKafkaConsumerClient(ctx context.Context, bConfig *BrokerConfig, options *ConsumerClientOptions) (Consumer, error) {
	err := validateKafkaConsumerBrokerConfig(bConfig)
	if err != nil {
		return nil, err
	}

	err = validateKafkaConsumerClientConfig(options)
	if err != nil {
		return nil, err
	}

	configMap := &kakfapkg.ConfigMap{
		"bootstrap.servers":  strings.Join(bConfig.Brokers, ","),
		"group.id":           options.GroupID,
		"auto.offset.reset":  "earliest",
		"session.timeout.ms": 6000,
		//"enable.auto.commit":     false,
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

	c, err := kakfapkg.NewConsumer(configMap)

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

	configMap := &kakfapkg.ConfigMap{
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

	p, err := kakfapkg.NewProducer(configMap)
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

	configMap := &kakfapkg.ConfigMap{
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

	a, err := kakfapkg.NewAdminClient(configMap)

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

	topics := make([]kakfapkg.TopicSpecification, 0)
	ts := kakfapkg.TopicSpecification{
		Topic:             tp,
		NumPartitions:     request.NumPartitions,
		ReplicationFactor: 1,
	}
	topics = append(topics, ts)
	topicsResp, tErr := k.Admin.CreateTopics(ctx, topics, nil)

	for _, tp := range topicsResp {
		if tp.Error.Code() != kakfapkg.ErrNoError {
			logger.Ctx(ctx).Error("kafka topic creation failed", "error", tp.Error.Error())
			return CreateTopicResponse{
				Response: topicsResp,
			}, fmt.Errorf("kafka: %v", tp.Error.String())
		}
	}

	logger.Ctx(ctx).Infow("kafka topic creation successfully completed", "response", topicsResp)

	return CreateTopicResponse{
		Response: topicsResp,
	}, tErr
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

// SendMessages sends a message on the topic
func (k *KafkaBroker) SendMessages(ctx context.Context, request SendMessageToTopicRequest) (SendMessageToTopicResponse, error) {

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

	deliveryChan := make(chan kakfapkg.Event)

	tp := normalizeTopicName(request.Topic)
	err := k.Producer.Produce(&kakfapkg.Message{
		TopicPartition: kakfapkg.TopicPartition{Topic: &tp, Partition: kakfapkg.PartitionAny},
		Value:          request.Message,
		Headers:        kHeaders,
	}, deliveryChan)

	var m *kakfapkg.Message
	select {
	case err := <-deliveryChan:
		m = err.(*kakfapkg.Message)
	case <-time.After(time.Duration(k.POptions.TimeoutSec) * time.Second):
		return SendMessageToTopicResponse{}, fmt.Errorf("failed to produce message to topic [%v] due to timeout [%v]", &request.Topic, k.POptions.TimeoutSec)
	}

	if m != nil && m.TopicPartition.Error != nil {
		return SendMessageToTopicResponse{
			MessageID: m.String(),
			Response:  err,
		}, m.TopicPartition.Error
	}

	close(deliveryChan)

	return SendMessageToTopicResponse{MessageID: m.String()}, err
}

//ReceiveMessages gets tries to get the number of messages mentioned in the param "numOfMessages"
//from the previous committed offset. If the available messages in the queue are less, returns
// how many ever messages are available
func (k *KafkaBroker) ReceiveMessages(ctx context.Context, request GetMessagesFromTopicRequest) (GetMessagesFromTopicResponse, error) {

	interval := k.Config.Consumer.PollIntervalSec
	if interval == 0 {
		interval = request.TimeoutSec
	}

	msgs := make(map[string]string, 0)
	for {
		msg, err := k.Consumer.ReadMessage(time.Duration(10) * time.Second)
		if err == nil {
			msgs[fmt.Sprintf("%v", int64(msg.TopicPartition.Offset))] = string(msg.Value)
			if len(msgs) == request.NumOfMessages {
				return GetMessagesFromTopicResponse{OffsetWithMessages: msgs}, nil
			}

		} else {
			// The client will automatically try to recover from all errors.
			return GetMessagesFromTopicResponse{
				Response: err,
			}, err
		}
	}
	return GetMessagesFromTopicResponse{OffsetWithMessages: msgs}, nil
}

// CommitByPartitionAndOffset Commits messages if any
//This func will commit the message consumed
//by all the previous calls to GetMessages
func (k *KafkaBroker) CommitByPartitionAndOffset(ctx context.Context, request CommitOnTopicRequest) (CommitOnTopicResponse, error) {
	tp := kakfapkg.TopicPartition{
		Topic: &request.Topic,
		//Partition: request.Partition,
		Offset: kakfapkg.Offset(request.Offset),
	}

	tps := make([]kakfapkg.TopicPartition, 0)
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
