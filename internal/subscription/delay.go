package subscription

// subs-delay-30-seconds, subs-delay-60-seconds ... subs-delay-600-seconds
const delayTopicNameFormat = "%v.delay.%v.seconds"

// projects/p1/topics/subs.delay.30.seconds
const delayTopicWithProjectNameFormat = "projects/%v/topics/%v.delay.%v.seconds"

// subs.delay.30.seconds-0-cg
const delayConsumerGroupIDFormat = "%v-%d-cg"

// delayTopicName-subscriberID
const delayConsumerGroupInstanceIDFormat = "%v-%v"

// Interval is internal delay type per allowed interval
type Interval uint

var (
	// Delay5sec 5sec
	Delay5sec Interval = 5
	// Delay30sec 30sec
	Delay30sec Interval = 30
	// Delay60sec 1min
	Delay60sec Interval = 60
	// Delay150sec 2.5min
	Delay150sec Interval = 150
	// Delay300sec 5min
	Delay300sec Interval = 300
	// Delay600sec 10min
	Delay600sec Interval = 600
	// Delay1800sec 30min
	Delay1800sec Interval = 1800
	// Delay3600sec 60min
	Delay3600sec Interval = 3600
)

var (
	// MinDelay ...
	MinDelay = Delay5sec
	// MaxDelay ...
	MaxDelay = Delay3600sec
)

// Intervals during subscription creation, query from the allowed intervals list, and create all the needed topics for retry.
var Intervals = []Interval{Delay5sec, Delay30sec, Delay60sec, Delay150sec, Delay300sec, Delay600sec, Delay1800sec, Delay3600sec}
