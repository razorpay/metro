// +build unit

package retry

import (
	"context"
	"testing"

	"github.com/razorpay/metro/internal/subscription"
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

	availableDelayIntervals := subscription.Intervals
	for _, test := range tests {
		actualIntervals := findAllRetryIntervals(test.min, test.max, test.currentRetryCount, test.maxRetryCount, test.currentInterval, availableDelayIntervals)
		assert.Equal(t, test.want, actualIntervals)
	}
}

// helper function used in testcases to calculate all the retry intervals
func findAllRetryIntervals(min, max, currentRetryCount, maxRetryCount, currentInterval int, availableDelayIntervals []subscription.Interval) []float64 {
	ctx := context.Background()
	expectedIntervals := make([]float64, 0)
	delayCalculator, _ := getDelayCalculator(ctx, exponentialDelayType)
	for currentRetryCount <= maxRetryCount {
		nextDelayInterval, _ := delayCalculator.CalculateNextDelayInterval(
			ctx,
			subscription.Interval(min),
			subscription.Interval(max),
			subscription.Interval(currentInterval),
			availableDelayIntervals,
			uint(currentRetryCount),
		)
		// calculateNextUsingExponentialBackoff(float64(min), float64(currentInterval), float64(currentRetryCount))
		closestInterval := float64(findClosestDelayInterval(uint(min), uint(max), availableDelayIntervals, float64(nextDelayInterval)))
		expectedIntervals = append(expectedIntervals, closestInterval)
		currentInterval = int(closestInterval)
		currentRetryCount++
	}
	return expectedIntervals
}
