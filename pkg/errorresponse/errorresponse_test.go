package errorresponse

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPublic(t *testing.T) {
	tests := []struct {
		name     string
		err      Error
		expected string
	}{
		// todo: should we allow this case? ideally, all public errors should have a code and desc.
		{
			name:     "all_empty",
			err:      Error{},
			expected: `{"error":{"code":"","description":"","step":null,"reason":null,"field":null,"source":null,"metadata":{}}}`,
		},

		{
			name:     "code_and_desc",
			err:      Error{Error: Public{Code: "BAD_REQUEST_ERROR", Description: "Bad request"}},
			expected: `{"error":{"code":"BAD_REQUEST_ERROR","description":"Bad request","step":null,"reason":null,"field":null,"source":null,"metadata":{}}}`,
		},

		{
			name: "payment_flow_fields",
			err: Error{
				Error: Public{
					Code:        "BAD_REQUEST_ERROR",
					Description: "Bad request",
					Step:        String("payment_debit_request"),
					Reason:      String("payment_declined"),
					Source:      String("issuer_bank"),
				},
			},
			expected: `{"error":{"code":"BAD_REQUEST_ERROR","description":"Bad request","step":"payment_debit_request","reason":"payment_declined","field":null,"source":"issuer_bank","metadata":{}}}`,
		},

		{
			name: "with_field_and_meta",
			err: Error{
				Error: Public{
					Code:        "BAD_REQUEST_VALIDATION_FAILURE",
					Description: "Validation Failure",
					Field:       String("address"),
					Metadata:    map[string]string{"operation": "random"},
				},
			},
			expected: `{"error":{"code":"BAD_REQUEST_VALIDATION_FAILURE","description":"Validation Failure","step":null,"reason":null,"field":"address","source":null,"metadata":{"operation":"random"}}}`,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			bytes, err := json.Marshal(test.err)
			assert.Nil(t, err)
			assert.Equal(t, test.expected, string(bytes))
		})
	}
}

func TestWithInternal(t *testing.T) {
	tests := []struct {
		name     string
		err      Error
		expected string
	}{
		{
			name: "no_internal",
			err: Error{
				Error: Public{},
			},
			expected: `{"error":{"code":"","description":"","step":null,"reason":null,"field":null,"source":null,"metadata":{}}}`,
		},

		{
			name: "empty_internal",
			err: Error{
				InternalError: &Internal{},
			},
			expected: `{"error":{"code":"","description":"","step":null,"reason":null,"field":null,"source":null,"metadata":{}},"internal_error":{"code":""}}`,
		},

		{
			name: "internal_no_public",
			err: Error{
				InternalError: &Internal{
					Code: "bad_request_exception",
				},
			},
			expected: `{"error":{"code":"","description":"","step":null,"reason":null,"field":null,"source":null,"metadata":{}},"internal_error":{"code":"bad_request_exception"}}`,
		},

		{
			name: "internal_with_metadata",
			err: Error{
				InternalError: &Internal{
					Code:        "bad_request_exception",
					Description: "something bad happened",
					Metadata:    map[string]string{"key": "value"},
				},
			},
			expected: `{"error":{"code":"","description":"","step":null,"reason":null,"field":null,"source":null,"metadata":{}},"internal_error":{"code":"bad_request_exception","description":"something bad happened","metadata":{"key":"value"}}}`,
		},

		{
			name: "all_in",
			err: Error{
				Error: Public{
					Code:        "BAD_REQUEST_ERROR",
					Description: "Bad request",
					Step:        String("payment_debit_request"),
					Reason:      String("payment_declined"),
					Source:      String("issuer_bank"),
					Field:       String("address"),
					Metadata:    map[string]string{"key": "value"},
				},
				InternalError: &Internal{
					Code:        "bad_request_exception",
					Description: "something bad happened",
					Metadata:    map[string]string{"key": "value"},
				},
			},
			expected: `{"error":{"code":"BAD_REQUEST_ERROR","description":"Bad request","step":"payment_debit_request","reason":"payment_declined","field":"address","source":"issuer_bank","metadata":{"key":"value"}},"internal_error":{"code":"bad_request_exception","description":"something bad happened","metadata":{"key":"value"}}}`,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			bytes, err := json.Marshal(test.err)
			assert.Nil(t, err)
			assert.Equal(t, test.expected, string(bytes))
		})
	}
}
