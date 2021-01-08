package registry

// HandlerFunc provides interface for the watch handler func
// which is implemented by watch subscriber
type HandlerFunc func([]Pair)

// WatchConfig struct provides watch details on registry
type WatchConfig struct {
	WatchType string
	WatchPath string
	Handler   HandlerFunc
}

// IWatcher defines the watch interface for watch over registry
type IWatcher interface {
	// StartWatch interface implemented by registry watcher to start a watch
	StartWatch() error

	// StopWatch interface implemented by registry watcher to stop a watch
	// it should also release the resources held
	StopWatch()
}
