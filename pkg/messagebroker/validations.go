package messagebroker

import "errors"

// validateKafkaConsumerClientConfig validates kafka consumer client config
func validateKafkaConsumerClientConfig(options *ConsumerClientOptions) error {

	if options.Topic == "" {
		return errors.New("kafka: empty topic name")
	}

	if options.GroupID == "" {
		return errors.New("kafka: empty group_id name")
	}

	return nil
}

// validateKafkaConsumerBrokerConfig validates kafka consumer broker config
func validateKafkaConsumerBrokerConfig(config *BrokerConfig) error {

	if config.Brokers == nil || len(config.Brokers) == 0 {
		return errors.New("kafka: empty brokers list")
	}

	if config.ConnectionTimeout > 60 {
		return errors.New("kafka: connection timeout above the allowed value of 60secs")
	}

	if config.ConnectionTimeout > 60 {
		return errors.New("kafka: operation timeout above the allowed value of 60secs")
	}

	return nil
}

// validateKafkaProducerClientConfig validates kafka producer client config
func validateKafkaProducerClientConfig(options *ProducerClientOptions) error {

	if options.Topic == "" {
		return errors.New("kafka: empty topic name")
	}

	return nil
}

// validateKafkaProducerBrokerConfig validates kafka producer broker config
func validateKafkaProducerBrokerConfig(config *BrokerConfig) error {

	if config.Brokers == nil || len(config.Brokers) == 0 {
		return errors.New("kafka: empty brokers list")
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
		return errors.New("kafka: empty brokers list")
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
		return errors.New("pulsar: empty brokers list")
	}

	if len(config.Brokers) > 1 {
		return errors.New("pulsar: should have only one broker during init")
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
		return errors.New("pulsar: empty brokers list")
	}

	if len(config.Brokers) > 1 {
		return errors.New("pulsar: should have only one broker during init")
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
		return errors.New("pulsar: empty brokers list")
	}

	if len(config.Brokers) > 1 {
		return errors.New("pulsar: should have only one broker during init")
	}

	return nil
}
