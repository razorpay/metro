// +build unit

package messagebroker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_readKafkaCerts_Success(t *testing.T) {
	configDir := "testdata/"
	certs, err := readKafkaCerts(configDir)

	assert.NotNil(t, certs)
	assert.Nil(t, err)
	assert.NotEmpty(t, certs.caCertPath)
	assert.NotEmpty(t, certs.userCertPath)
	assert.NotEmpty(t, certs.userKeyPath)
}

func Test_readKafkaCerts_Failure(t *testing.T) {
	testDirs := []string{
		"testdata1", "testdata2", "testdata3",
	}

	for _, dir := range testDirs {
		certs, err := readKafkaCerts(dir)
		assert.NotNil(t, err)
		assert.Nil(t, certs)
	}
}

func Test_Pause(t *testing.T) {
	c := getConsumer()

	err := c.Pause(context.Background(), PauseOnTopicRequest{
		Topic:     "t1",
		Partition: 0,
	})

	assert.Nil(t, err)
}

func Test_DescribeTopicConfigs(t *testing.T) {
	a := getKafkaAdmin()
	resp, err := a.DescribeTopicConfigs(context.Background(), []string{})
	assert.NotNil(t, err)
	assert.Nil(t, resp)
}

func Test_AlterTopicConfigs(t *testing.T) {
	a := getKafkaAdmin()
	resp, err := a.AlterTopicConfigs(context.Background(), ModifyTopicConfigRequest{TopicConfigs: nil})
	assert.NotNil(t, err)
	assert.Nil(t, resp)
}

func getConsumer() Consumer {
	consumer, _ := newKafkaConsumerClient(context.Background(), getValidBrokerConfig(), getValidConsumerClientOptions())
	return consumer
}

func getKafkaAdmin() Admin {
	admin, _ := newKafkaAdminClient(context.Background(), getValidBrokerConfig(), getValidAdminClientOptions())
	return admin
}

func getValidBrokerConfig() *BrokerConfig {
	return &BrokerConfig{
		Brokers:             []string{"b1", "b2"},
		EnableTLS:           false,
		DebugEnabled:        false,
		OperationTimeoutMs:  100,
		ConnectionTimeoutMs: 100,
	}
}

func getValidConsumerClientOptions() *ConsumerClientOptions {
	return &ConsumerClientOptions{
		Topics:          []string{"t1"},
		Subscription:    "s1",
		GroupID:         "sg1",
		GroupInstanceID: "sg_i1",
	}
}
func getValidProducerClientOptions() *ProducerClientOptions {
	return &ProducerClientOptions{
		Topic:     "t1",
		TimeoutMs: 1000,
	}
}

func getValidAdminClientOptions() *AdminClientOptions {
	return &AdminClientOptions{}
}
