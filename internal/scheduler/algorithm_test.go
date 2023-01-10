//go:build unit
// +build unit

package scheduler

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetAlgorithmImpl(t *testing.T) {
	tests := []struct {
		algorithm Algorithm
		err       error
	}{
		{
			algorithm: Random,
			err:       nil,
		},
		{
			algorithm: LoadBalance,
			err:       nil,
		},
		{
			algorithm: "invalid",
			err:       ErrInvalidAlgorithm,
		},
	}

	for _, test := range tests {
		ai, err := GetAlgorithmImpl(test.algorithm)
		assert.Equal(t, err, test.err)

		if test.err == nil {
			assert.NotNil(t, ai)
		}
	}
}
