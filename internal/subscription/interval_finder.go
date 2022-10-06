package subscription

import (
	"github.com/razorpay/metro/internal/topic"
)

// IntervalFinder defines the next interval identification logic
type IntervalFinder interface {
	Next(IntervalFinderParams) topic.Interval
}

// IntervalFinderParams defines the constraints to be used to identify the next interval
type IntervalFinderParams struct {
	min           uint
	max           uint
	delayInterval float64
	intervals     []topic.Interval
}

func NewIntervalFinderParams(min, max uint, delayInterval float64, intervals []topic.Interval) IntervalFinderParams {
	return IntervalFinderParams{
		min:           min,
		max:           max,
		delayInterval: delayInterval,
		intervals:     intervals,
	}
}

// NewClosestIntervalWithCeil  returns the closest interval window finder
func NewClosestIntervalWithCeil() IntervalFinder {
	return closestIntervalWithCeil{}
}

// returns the minimum interval greater or equal to the given delay interval
type closestIntervalWithCeil struct{}

func (closestIntervalWithCeil) Next(i IntervalFinderParams) topic.Interval {
	newDelay := i.delayInterval
	// restrict newDelay based on the given min-max boundary conditions
	if newDelay < float64(i.min) {
		newDelay = float64(i.min)
	} else if newDelay > float64(i.max) {
		newDelay = float64(i.max)
	}

	// find the closest interval greater-equal to newDelay
	for _, interval := range i.intervals {
		if float64(interval) >= newDelay {
			return interval
		}
	}

	// by default use the max available delay
	return topic.MaxDelay
}
