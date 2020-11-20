package producer

type Core struct {
	producer IProducer
}

func NewCore(producer IProducer) (*Core, error) {
	return &Core{producer: producer}, nil
}

type Client interface {
	PublishMessage(topic string, body []byte) (string, error)
	AckMessage(msgId string) error
	NackMessage(msgId string) error
}

func (c *Core) PublishMessage(topic string, body []byte) (string, error) {
	return c.producer.PublishMessage(topic, body)
}
