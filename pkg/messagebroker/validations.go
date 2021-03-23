package messagebroker

import (
	"fmt"
)

// validateKafkaConsumerClientConfig validates kafka consumer client config
func validateKafkaConsumerClientConfig(options *ConsumerClientOptions) error {

	if options.Topics == nil || len(options.Topics) == 0 {
		return fmt.Errorf("kafka: empty topic name")
	}

	if options.GroupID == "" {
		return fmt.Errorf("kafka: empty group_id name")
	}

	return nil
}

// validateKafkaConsumerBrokerConfig validates kafka consumer broker config
func validateKafkaConsumerBrokerConfig(config *BrokerConfig) error {

	if config.Brokers == nil || len(config.Brokers) == 0 {
		return fmt.Errorf("kafka: empty brokers list")
	}

	if config.ConnectionTimeoutSec > 60 {
		return fmt.Errorf("kafka: connection timeout above the allowed value of 60secs")
	}

	if config.OperationTimeoutSec > 60 {
		return fmt.Errorf("kafka: operation timeout above the allowed value of 60secs")
	}

	return nil
}

// validateKafkaProducerClientConfig validates kafka producer client config
func validateKafkaProducerClientConfig(options *ProducerClientOptions) error {

	if options.Topic == "" {
		return fmt.Errorf("kafka: empty topic name")
	}

	if options.Partition < 0 {
		return fmt.Errorf("kafka: invalid partition")
	}

	if options.TimeoutSec > 60 {
		return fmt.Errorf("kafka: operation timeout above the allowed value of 60secs")
	}

	return nil
}

// validateKafkaProducerBrokerConfig validates kafka producer broker config
func validateKafkaProducerBrokerConfig(config *BrokerConfig) error {

	if config.Brokers == nil || len(config.Brokers) == 0 {
		return fmt.Errorf("kafka: empty brokers list")
	}

	return nil
}

// validateKafkaAdminClientConfig validates kafka admin client config
func validateKafkaAdminClientConfig(options *AdminClientOptions) error {
	return nil
}

// validateKafkaAdminBrokerConfig validates kafka admin broker config
func validateKafkaAdminBrokerConfig(config *BrokerConfig) error {

	if config.Brokers == nil || len(config.Brokers) == 0 {
		return fmt.Errorf("kafka: empty brokers list")
	}

	return nil
}

// validatePulsarConsumerClientConfig validates pulsar consumer client config
func validatePulsarConsumerClientConfig(options *ConsumerClientOptions) error {
	return nil
}

// validatePulsarConsumerBrokerConfig validates pulsar consumer broker config
func validatePulsarConsumerBrokerConfig(config *BrokerConfig) error {

	if config.Brokers == nil || len(config.Brokers) == 0 {
		return fmt.Errorf("pulsar: empty brokers list")
	}

	if len(config.Brokers) > 1 {
		return fmt.Errorf("pulsar: should have only one broker during init")
	}

	return nil
}

// validatePulsarProducerClientConfig validates pulsar producer client config
func validatePulsarProducerClientConfig(options *ProducerClientOptions) error {
	return nil
}

// validatePulsarProducerBrokerConfig validates pulsar producer broker config
func validatePulsarProducerBrokerConfig(config *BrokerConfig) error {

	if config.Brokers == nil || len(config.Brokers) == 0 {
		return fmt.Errorf("pulsar: empty brokers list")
	}

	if len(config.Brokers) > 1 {
		return fmt.Errorf("pulsar: should have only one broker during init")
	}

	return nil
}

// validatePulsarAdminClientConfig validates pulsar admin client config
func validatePulsarAdminClientConfig(options *AdminClientOptions) error {
	return nil
}

// validatePulsarAdminBrokerConfig validates pulsar admin broker config
func validatePulsarAdminBrokerConfig(config *BrokerConfig) error {

	if config.Brokers == nil || len(config.Brokers) == 0 {
		return fmt.Errorf("pulsar: empty brokers list")
	}

	if len(config.Brokers) > 1 {
		return fmt.Errorf("pulsar: should have only one broker during init")
	}

	return nil
}
