package producer

type Core struct {
	producer IProducer
}

func NewCore(producer IProducer) (*Core, error) {
	return &Core{producer: producer}, nil
}

func (c *Core) PublishMessage(topic string, body []byte) (string, error) {
	return c.producer.PublishMessage(&Message{
		topic:   topic,
		message: body,
	})
}
