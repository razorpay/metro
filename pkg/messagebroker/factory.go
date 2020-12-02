package messagebroker

const (
	Kafka = iota
	Pulsar
)

func NewBroker(identifier string) Broker {
	switch identifier {
	case string(Kafka):
		return NewKafkaBroker(nil, nil)
	case string(Pulsar):
		return NewPulsarBroker(nil, nil)
	}

	panic("invalid identifier")
}
