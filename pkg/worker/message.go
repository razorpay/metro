package worker

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"errors"
)

var (
	ErrorEmptyContent             = errors.New("content is empty")
	ErrorNilJobSerialize          = errors.New("can not serialize nil job")
	ErrorDataTypeMismatch         = errors.New("failed to assert data type of serialized message")
	ErrorJobConstructorNotDefined = errors.New("there is no job constructor defined")
)

// Message payload representation of jon
type Message struct {
	// Serialized true if it containes encoded job
	Serialized bool `json:"serialized"`

	// Content message content
	Content string `json:"content"`
}

type IMessage interface {
	IsSerialized() bool
	GetContent() string
	Serialize(job IJob) error
	Load(str string) error
	Deserialize() (IJob, error)
	ConstructJob() (IJob, error)
	GetJob() (IJob, error)
}

// IsSerialized returns true of the message has serialized job
func (m *Message) IsSerialized() bool {
	return m.Serialized
}

// GetContent gived the content of the message
func (m *Message) GetContent() string {
	return m.Content
}

// Marshal serialize the message into json
func (m *Message) Marshal() string {
	b, _ := json.Marshal(m)

	return string(b)
}

// Load load the raw message into message struct
func (m *Message) Load(str string) error {
	var content map[string]interface{}

	if err := json.Unmarshal([]byte(str), &content); err != nil {
		return err
	}

	if v, ok := content["serialized"]; ok {
		isSerialized, bok := v.(bool)
		strContent, cok := content["content"].(string)

		if !bok || !cok {
			return ErrorDataTypeMismatch
		}

		m.Serialized = isSerialized
		m.Content = strContent
	} else {
		m.Serialized = false
		m.Content = str
	}

	return nil
}

// Serialize encode the job provided and fill the required fields accordingly
func (m *Message) Serialize(job IJob) error {
	var buffer bytes.Buffer

	if job == nil {
		return ErrorNilJobSerialize
	}

	encoder := gob.NewEncoder(&buffer)

	if err := encoder.Encode(&job); err != nil {
		return errors.New("serialization failed " + err.Error())
	}

	m.Serialized = true
	m.Content = base64.StdEncoding.EncodeToString(buffer.Bytes())

	return nil
}

// Deserialize construct the job from the encoded message
func (m *Message) Deserialize() (IJob, error) {
	var job IJob

	if m.GetContent() == "" {
		return nil, ErrorEmptyContent
	}

	var buffer bytes.Buffer

	value, err := base64.StdEncoding.DecodeString(m.GetContent())
	if err != nil {
		return nil, errors.New("failed while decoding the source " + err.Error())
	}

	buffer.Write(value)
	decoder := gob.NewDecoder(&buffer)

	if err = decoder.Decode(&job); err != nil {
		return nil, errors.New("failed to deserialize " + err.Error())
	}

	return job, err
}

// ConstructJob will create the job from the raw encoded message
func (m *Message) ConstructJob(constructor Constructor) (IJob, error) {
	if constructor == nil {
		return nil, ErrorJobConstructorNotDefined
	}

	job, err := constructor(m.Content)

	return job, err
}

// GetJob returns the job based on the message received
// if the message has serialized job then creates associated job
// else create a job from raw message
func (m *Message) GetJob(handler JobHandler) (IJob, error) {
	if m.IsSerialized() {
		return m.Deserialize()
	}

	return m.ConstructJob(handler.Constructor)
}
