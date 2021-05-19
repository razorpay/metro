package message

// Message struct is used for functional testing, messages for pubsub are defined using this struct
type Message struct {
	Data               []byte
	ResponseCode       int
	RetryResponseCodes []int
	Checksum           string
}
