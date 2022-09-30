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
	admin, _ := newPulsarAdminClient(context.Background(), getValidBrokerConfig(), getValidAdminClientOptions())
	return admin
}
