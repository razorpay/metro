// +build unit

package retry

import (
	"testing"

	"github.com/razorpay/metro/internal/topic"
	"github.com/stretchr/testify/assert"
)

func Test_ClosestIntervalWithCeil(t *testing.T) {

	finder := NewClosestIntervalWithCeil()

	inputIntervals := []float64{7, 90, 210, 430, 700, 1000, 4000}
	expectedIntervals := []float64{30, 150, 300, 600, 1800, 1800, 3600}
	actualIntervals := make([]float64, 0)

	for _, inputInterval := range inputIntervals {
		closestInterval := finder.Next(IntervalFinderParams{
			min:           10,
			max:           3600,
			delayInterval: inputInterval,
			intervals:     topic.Intervals,
		})
		actualIntervals = append(actualIntervals, float64(closestInterval))
	}
	assert.Equal(t, expectedIntervals, actualIntervals)
}
