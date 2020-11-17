package worker_test

import (
	"encoding/gob"
	"errors"
	"reflect"
	"testing"

	"github.com/razorpay/metro/pkg/worker"
	"github.com/stretchr/testify/assert"
)

func TestMessage_Load(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
		err         string
		errType     string
		msg         worker.Message
	}{
		{
			name:        "Unmarshal failure",
			input:       "qwe",
			expectError: true,
			err:         "invalid character 'q' looking for beginning of value",
			errType:     "*json.SyntaxError",
			msg:         worker.Message{},
		},
		{
			name:        "invalid content",
			input:       `{"serialized":true}`,
			expectError: true,
			err:         "failed to assert data type of serialized message",
			errType:     "*errors.errorString",
			msg:         worker.Message{},
		},
		{
			name:        "serialized content",
			input:       `{"serialized":true, "content":"some content"}`,
			expectError: false,
			err:         "",
			errType:     "",
			msg: worker.Message{
				Serialized: true,
				Content:    "some content",
			},
		},
		{
			name:        "raw content",
			input:       `{"some":"thing"}`,
			expectError: false,
			err:         "",
			errType:     "",
			msg: worker.Message{
				Serialized: false,
				Content:    `{"some":"thing"}`,
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			m := &worker.Message{}
			err := m.Load(testCase.input)
			if testCase.expectError {
				assert.NotNil(t, err)
				assert.Equal(t, testCase.err, err.Error())
				assert.Equal(t, testCase.errType, reflect.TypeOf(err).String())
			} else {
				assert.Nil(t, err)
			}
			assert.Equal(t, testCase.msg, *m)
		})
	}
}

func TestMessage_Deserialize(t *testing.T) {
	tests := []struct {
		name        string
		input       *worker.Message
		expectError bool
		err         string
		errType     string
		job         worker.IJob
		serialized  bool
	}{
		{
			name: "empty content",
			input: &worker.Message{
				Serialized: true,
				Content:    "",
			},
			expectError: true,
			err:         "content is empty",
			errType:     "*errors.errorString",
			job:         nil,
			serialized:  false,
		},
		{
			name: "decode error",
			input: &worker.Message{
				Serialized: true,
				Content:    "1",
			},
			expectError: true,
			err:         "failed while decoding the source illegal base64 data at input byte 0",
			errType:     "*errors.errorString",
			job:         nil,
			serialized:  false,
		},
		{
			name: "success",
			input: &worker.Message{
				Serialized: true,
				Content:    "LRAAECp3b3JrZXJfdGVzdC5Kb2L/gQMBAQNKb2IB/4IAAQEBA0pvYgH/hAAAAFX/gwMBAQNKb2IB/4QAAQYBBVF1ZXVlAQwAAQRBcmdzAf+GAAEETmFtZQEMAAEKTWF4UmV0cmllcwEEAAEHQXR0ZW1wdAEEAAEHVGltZW91dAEEAAAAFP+FBAEBBEFyZ3MB/4YAAQwBEAAABv+CAwEAAA==",
			},
			expectError: false,
			err:         "",
			errType:     "",
			job:         &Job{},
			serialized:  true,
		},
		{
			name: "decode failure",
			input: &worker.Message{
				Serialized: true,
				Content:    "RRAAHCp3b3JrZXJfdGVzdC5VbnJlZ2lzdGVyZWRKb2L/gQMBAQ9VbnJlZ2lzdGVyZWRKb2IB/4IAAQEBA0pvYgH/hAAAAFX/gwMBAQNKb2IB/4QAAQYBBVF1ZXVlAQwAAQRBcmdzAf+GAAEETmFtZQEMAAEKTWF4UmV0cmllcwEEAAEHQXR0ZW1wdAEEAAEHVGltZW91dAEEAAAAFP+FBAEBBEFyZ3MB/4YAAQwBEAAABv+CAwEAAA==",
			},
			expectError: true,
			err:         `failed to deserialize gob: name not registered for interface: "*worker_test.UnregisteredJob"`,
			errType:     "*errors.errorString",
			job:         &UnregisteredJob{},
			serialized:  false,
		},
	}

	gob.Register(&Job{})

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			job, err := testCase.input.Deserialize()
			if testCase.expectError {
				assert.NotNil(t, err)
				assert.Equal(t, testCase.err, err.Error())
				if testCase.errType != "" {
					assert.Equal(t, testCase.errType, reflect.TypeOf(err).String())
				}
			} else {
				assert.Nil(t, err)
				assert.Equal(t, testCase.job, job)
			}
		})
	}
}

func TestMessage_Serialize(t *testing.T) {
	tests := []struct {
		name        string
		input       worker.IJob
		expectError bool
		err         string
		errType     string
		msg         worker.Message
		mock        func()
	}{
		{
			name:        "serialize job success 1",
			input:       &DelayedJob{},
			expectError: false,
			err:         "",
			errType:     "",
			msg: worker.Message{
				Serialized: true,
				Content:    "OxAAFyp3b3JrZXJfdGVzdC5EZWxheWVkSm9i/4EDAQEKRGVsYXllZEpvYgH/ggABAQEDSm9iAf+EAAAAT/+DAwEBA0pvYgH/hAABBQEJUXVldWVOYW1lAQwAAQROYW1lAQwAAQpNYXhSZXRyaWVzAQQAAQdBdHRlbXB0AQQAAQdUaW1lb3V0AQQAAAAG/4IDAQAA",
			},
			mock: func() {
				gob.Register(&DelayedJob{})
			},
		},
		{
			name:        "serialize job success 2",
			input:       &FailureJob{worker.Job{Name: "failure_job"}},
			expectError: false,
			err:         "",
			errType:     "",
			msg: worker.Message{
				Serialized: true,
				Content:    "OxAAFyp3b3JrZXJfdGVzdC5GYWlsdXJlSm9i/4UDAQEKRmFpbHVyZUpvYgH/hgABAQEDSm9iAf+EAAAAT/+DAwEBA0pvYgH/hAABBQEJUXVldWVOYW1lAQwAAQROYW1lAQwAAQpNYXhSZXRyaWVzAQQAAQdBdHRlbXB0AQQAAQdUaW1lb3V0AQQAAAAT/4YQAQILZmFpbHVyZV9qb2IAAA==",
			},
			mock: func() {
				gob.Register(&FailureJob{})
			},
		},
		{
			name:        "serialize failure 1",
			input:       nil,
			expectError: true,
			err:         "can not serialize nil job",
			errType:     "*errors.errorString",
			msg:         worker.Message{},
			mock:        func() {},
		},
		{
			name:        "serialize failure 2",
			input:       &UnregisteredJob{},
			expectError: true,
			err:         "serialization failed gob: type not registered for interface: worker_test.UnregisteredJob",
			errType:     "*errors.errorString",
			msg:         worker.Message{},
			mock:        func() {},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			testCase.mock()
			m := &worker.Message{}
			err := m.Serialize(testCase.input)
			if testCase.expectError {
				assert.NotNil(t, err)
				assert.Equal(t, testCase.err, err.Error())
				assert.Equal(t, testCase.errType, reflect.TypeOf(err).String())
			} else {
				assert.Nil(t, err)
			}
			assert.Equal(t, len(testCase.msg.Content), len(m.Content))
		})
	}
}

func TestMessage_Marshal(t *testing.T) {
	m := &worker.Message{
		Serialized: false,
		Content:    "Some random message",
	}

	assert.Equal(t, `{"serialized":false,"content":"Some random message"}`, m.Marshal())
}

func TestMessage_GetJob(t *testing.T) {
	tests := []struct {
		name        string
		input       *worker.Message
		serialized  bool
		err         error
		job         worker.IJob
		constructor worker.Constructor
	}{
		{
			name: "construct job",
			input: &worker.Message{
				Serialized: false,
				Content:    "some thing",
			},
			serialized: false,
			err:        nil,
			job: &RawJob{
				content: "some thing",
				Job: worker.Job{
					Name:      "raw_job",
					QueueName: "temp_queue",
				},
			},
			constructor: (&RawJob{}).Construct,
		},
		{
			name: "nil constructor",
			input: &worker.Message{
				Serialized: false,
				Content:    "some thing",
			},
			serialized:  false,
			err:         errors.New("there is no job constructor defined"),
			job:         nil,
			constructor: nil,
		},
		{
			name: "nil constructor",
			input: &worker.Message{
				Serialized: true,
				Content:    "OxAAFyp3b3JrZXJfdGVzdC5EZWxheWVkSm9i/4cDAQEKRGVsYXllZEpvYgH/iAABAQEDSm9iAf+EAAAAS/+DAwEBA0pvYgH/hAABBQEFUXVldWUBDAABBE5hbWUBDAABCk1heFJldHJpZXMBBAABB0F0dGVtcHQBBAABB1RpbWVvdXQBBAAAAAb/iAMBAAA=",
			},
			serialized:  true,
			err:         nil,
			job:         &DelayedJob{},
			constructor: nil,
		},
	}

	gob.Register(&DelayedJob{})

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			job, err := testCase.input.GetJob(worker.JobHandler{
				Constructor: testCase.constructor,
			})

			assert.Equal(t, testCase.err, err)
			assert.Equal(t, testCase.job, job)
		})
	}
}
