package registry

// HandlerFunc provides interface for the watch handler func
// which is implemented by watch subscriber
type HandlerFunc func([]Pair)

// WatchHandler struct provides watch capability for registry
type WatchHandler struct {
	watchType string
	watchPath string
	handler   HandlerFunc
}
