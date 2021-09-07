// +build unit

package subscription

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_validateDelayConfig(t *testing.T) {

	type test struct {
		m                           *Model
		expectedMinimumBackoff      uint
		expectedMaximumBackoff      uint
		expectedMaxDeliveryAttempts int32
		err                         error
	}

	tests := []test{
		{
			m: &Model{
				RetryPolicy:      nil,
				DeadLetterPolicy: nil,
			},
			expectedMinimumBackoff:      10,
			expectedMaximumBackoff:      600,
			expectedMaxDeliveryAttempts: 5,
			err:                         nil,
		},
		{
			m: &Model{
				RetryPolicy: &RetryPolicy{
					MinimumBackoff: 30,
					MaximumBackoff: 600,
				},
				DeadLetterPolicy: &DeadLetterPolicy{
					MaxDeliveryAttempts: 10,
				},
			},
			expectedMinimumBackoff:      30,
			expectedMaximumBackoff:      600,
			expectedMaxDeliveryAttempts: 10,
			err:                         nil,
		},
		{
			m: &Model{
				RetryPolicy: &RetryPolicy{
					MinimumBackoff: 1000,
				},
			},
			err: ErrInvalidMinBackoff,
		},
		{
			m: &Model{
				RetryPolicy: &RetryPolicy{
					MaximumBackoff: 9999,
				},
			},
			err: ErrInvalidMaxBackoff,
		},
		{
			m: &Model{
				DeadLetterPolicy: &DeadLetterPolicy{
					MaxDeliveryAttempts: -40,
				},
			},
			err: ErrInvalidMaxDeliveryAttempt,
		},
		{
			m: &Model{
				DeadLetterPolicy: &DeadLetterPolicy{
					MaxDeliveryAttempts: 888,
				},
			},
			err: ErrInvalidMaxDeliveryAttempt,
		},
		{
			m: &Model{
				DeadLetterPolicy: &DeadLetterPolicy{
					MaxDeliveryAttempts: 0,
				},
			},
			expectedMinimumBackoff:      10,
			expectedMaximumBackoff:      600,
			expectedMaxDeliveryAttempts: 5,
			err:                         nil,
		},
	}

	for _, test := range tests {
		err := validateDelayConfig(test.m)
		assert.Equal(t, test.err, err)

		if err == nil {
			assert.NotNil(t, test.m.RetryPolicy)
			assert.NotNil(t, test.m.DeadLetterPolicy)
			assert.Equal(t, test.expectedMinimumBackoff, test.m.RetryPolicy.MinimumBackoff)
			assert.Equal(t, test.expectedMaximumBackoff, test.m.RetryPolicy.MaximumBackoff)
			assert.Equal(t, test.expectedMaxDeliveryAttempts, test.m.DeadLetterPolicy.MaxDeliveryAttempts)
		}
	}
}
