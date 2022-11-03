// +build unit

package subscription

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ExponentialWindowBackoff(t *testing.T) {

	nef := NewExponentialWindowBackoff()

	count := float64(1)
	lastInterval := float64(0)

	expectedIntervals := []float64{10, 30, 70, 150, 310}
	actualIntervals := make([]float64, 0)

	for count <= 5 {
		lastInterval = nef.Next(BackoffPolicy{
			startInterval: 10,
			lastInterval:  lastInterval,
			count:         count,
			exponential:   2,
		})
		actualIntervals = append(actualIntervals, lastInterval)
		count++
	}
	assert.Equal(t, expectedIntervals, actualIntervals)
}

func Test_FixedWindowBackoff(t *testing.T) {

	nef := NewFixedWindowBackoff()

	count := float64(1)
	lastInterval := float64(0)

	expectedIntervals := []float64{10, 20, 30, 40, 50}
	actualIntervals := make([]float64, 0)

	for count <= 5 {
		lastInterval = nef.Next(BackoffPolicy{
			startInterval: 10,
			count:         count,
		})
		actualIntervals = append(actualIntervals, lastInterval)
		count++
	}
	assert.Equal(t, expectedIntervals, actualIntervals)
}
