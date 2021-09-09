package retry

import "math"

// Backoff defines the backoff calculation logic
type Backoff interface {
	Next(BackoffPolicy) float64
}

// BackoffPolicy is a backoff policy for retrying an operation.
type BackoffPolicy struct {
	startInterval float64
	lastInterval  float64
	count         float64
	exponential   float64
}

// NewExponentialWindowBackoff  return a backoff policy that that grows exponentially.
func NewExponentialWindowBackoff() Backoff {
	return exponentialWindowBackoffParams{}
}

// NewFixedWindowBackoff  return a backoff policy that always returns the same backoff delay.
func NewFixedWindowBackoff() Backoff {
	return fixedWindowBackoffParams{}
}

// keeping this stateless. feed in last state on runtime and calculate next.
type exponentialWindowBackoffParams struct{}

// Using below formula
// EXPONENTIAL: nextDelayInterval = currentDelayInterval + (delayIntervalMinutes * 2^(retryCount-1))
//Refer http://exponentialbackoffcalculator.com/
func (e exponentialWindowBackoffParams) Next(b BackoffPolicy) float64 {
	return b.lastInterval + b.startInterval*math.Pow(2, b.count-1)
}

type fixedWindowBackoffParams struct{}

func (f fixedWindowBackoffParams) Next(b BackoffPolicy) float64 {
	return b.startInterval * b.count
}
