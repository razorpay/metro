package messagebroker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPulsarBroker_AlterTopicConfigs(t *testing.T) {
	defer func() {
		if recover() == nil {
			assert.Fail(t, "Alter topic config should have failed")
		}
	}()

	admin := getPulsarAdmin()
	admin.AlterTopicConfigs(context.Background(), ModifyTopicConfigRequest{})
}

func TestPulsarBroker_DescribeTopicConfigs(t *testing.T) {
	defer func() {
		if recover() == nil {
			assert.Fail(t, "describe topic config should have failed")
		}
	}()

	admin := getPulsarAdmin()
	admin.DescribeTopicConfigs(context.Background(), []string{})
}

func getPulsarAdmin() Admin {
	admin, _ := newPulsarAdminClient(context.Background(), getValidPulsarBrokerConfig(), &AdminClientOptions{})
	return admin
}

func getValidPulsarBrokerConfig() *BrokerConfig {
	return &BrokerConfig{
		Brokers:             []string{"b1"},
		EnableTLS:           false,
		DebugEnabled:        false,
		OperationTimeoutMs:  100,
		ConnectionTimeoutMs: 100,
	}
}
