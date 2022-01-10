// +build unit

package retry

import (
	"testing"

	"github.com/razorpay/metro/internal/topic"
	"github.com/stretchr/testify/assert"
)

type intervalTest struct {
	min, max                         int
	currentRetryCount, maxRetryCount int
	currentInterval                  int
	want                             []float64
}

func Test_Retrier(t *testing.T) {

}

func Test_Interval_Calculator(t *testing.T) {

	tests := []intervalTest{
		{
			min:               10,
			max:               600,
			currentRetryCount: 1,
			maxRetryCount:     5,
			currentInterval:   0,
			want:              []float64{30, 60, 150, 300, 600},
		},
		{
			min:               1,
			max:               5,
			currentRetryCount: 1,
			maxRetryCount:     5,
			currentInterval:   0,
			want:              []float64{5, 5, 5, 5, 5},
		},
		{
			min:               30,
			max:               150,
			currentRetryCount: 1,
			maxRetryCount:     4,
			currentInterval:   0,
			want:              []float64{30, 150, 150, 150},
		},
		{
			min:               300,
			max:               600,
			currentRetryCount: 1,
			maxRetryCount:     4,
			currentInterval:   0,
			want:              []float64{300, 600, 600, 600},
		},
		{
			min:               600,
			max:               600,
			currentRetryCount: 1,
			maxRetryCount:     5,
			currentInterval:   0,
			want:              []float64{600, 600, 600, 600, 600},
		},
		{
			min:               600,
			max:               3600,
			currentRetryCount: 1,
			maxRetryCount:     7,
			currentInterval:   0,
			want:              []float64{600, 1800, 3600, 3600, 3600, 3600, 3600},
		},
		{
			min:               1800,
			max:               3600,
			currentRetryCount: 1,
			maxRetryCount:     3,
			currentInterval:   0,
			want:              []float64{1800, 3600, 3600},
		},
	}

	availableDelayIntervals := topic.Intervals
	for _, test := range tests {
		actualIntervals := findAllRetryIntervals(test.min, test.max, test.currentRetryCount, test.maxRetryCount, test.currentInterval, availableDelayIntervals)
		assert.Equal(t, test.want, actualIntervals)
	}
}
