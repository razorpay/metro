package messagebroker

func NewBroker(identifier string) Broker {
	switch identifier {
	case "kafka":
		return NewKafkaBroker(nil, nil)
	case "pulsar":
		return NewPulsarBroker(nil, nil)
	}

	panic("invalid identifier")
}
