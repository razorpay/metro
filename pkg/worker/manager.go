package worker

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/razorpay/metro/pkg/worker/queue"
)

// Constructor function that will be run by the worker and given
// a slice of arguments to create job
type Constructor func(string) (IJob, error)

// JobHandler will contain
// 1. Constructor := used to create the job
// 2. QueueName : to identify from which queue the message has to process
type JobHandler struct {
	Constructor Constructor
	QueueName   string
}

// IManager defines worker interface
type IManager interface {
	// Start the worker with the given context
	Start(ctx context.Context) error

	// Stop the worker
	Stop() error

	// Perform will accept the job the dispatched it for processing via queue provided
	// this will also check for retry count and take action accordingly
	Perform(job IJob, delay time.Duration) error

	// Register a Handler
	Register(job IJob, constructor ...Constructor) error
}

// ILogger is used by the worker to write logs
type ILogger interface {
	Debug(string)
	Info(string)
	Error(string)
}

var (
	ErrorNoMessageReceived         = errors.New("no messages received")
	ErrorJobTimeout                = errors.New("job timed out")
	ErrorStartingUnregisteredJob   = errors.New("can not start worker to handle unregistered job")
	ErrorPerformingUnregisteredJob = errors.New("job should be registered before performing")
)

// Manager struct defined all necessary attribute required to perform operations
type Manager struct {
	// Done will receive a single once the worker is terminated successfully
	// this can be used to ensure graceful termination.
	Done chan bool

	// config holds the basic configurations of worker
	// like: concurrency, waitTime and handler identifier to be run
	config *Config

	// queue holds the queue provider to push/fetch Message
	queue queue.IQueue

	// logger provided logger will be used in all logging in the worker
	logger ILogger

	// ctx worker context
	ctx context.Context

	// cancel stores the context cancel handler
	cancel context.CancelFunc

	// handlers hold all the jobs registered with worker
	handlers map[string]JobHandler

	// identifier job identifier handled by the worker
	identifier string

	//wg weight group to ensure closure of all goroutines before terminating
	wg sync.WaitGroup

	sync.Mutex
}

// NewManager create a instance of worker manger which handler the worker executions
func NewManager(config *Config, queue queue.IQueue, logger ILogger) *Manager {
	config.SetDefaults()

	mgr := &Manager{
		config:     config,
		queue:      queue,
		logger:     logger,
		handlers:   make(map[string]JobHandler),
		Done:       make(chan bool, 1),
		identifier: config.Name,
	}

	return mgr
}

// Register Handler with the worker
// also registers the job for gob encoding
func (mgr *Manager) Register(job IJob, constructor ...Constructor) error {
	mgr.Lock()
	defer mgr.Unlock()

	if mgr.isRegistered(job.GetName()) {
		return errors.New("handler already mapped for name " + job.GetName())
	}

	msgConstructor := func() Constructor {
		if len(constructor) > 0 {
			return constructor[0]
		}
		return nil
	}()

	mgr.handlers[job.GetName()] = JobHandler{
		Constructor: msgConstructor,
		QueueName:   job.GetQueue(),
	}

	gob.Register(job)

	return nil
}

// Start will initialize the worker and start the processing
// this will handle the concurrency of worker based on the value provided in the config
// also it takes care of graceful termination of jobs
func (mgr *Manager) Start(ctx context.Context) error {
	if !mgr.isRegistered(mgr.config.Name) {
		return ErrorStartingUnregisteredJob
	}

	mgr.logger.Info("starting worker")

	mgr.ctx, mgr.cancel = context.WithCancel(ctx)
	defer mgr.cancel()

	mgr.wg.Add(mgr.config.MaxConcurrency)

	for i := 0; i < mgr.config.MaxConcurrency; i++ {
		go mgr.work()
	}

	mgr.wg.Wait()
	mgr.Done <- true

	return nil
}

// Stop will send the cancel signal to the processing
func (mgr *Manager) Stop() error {
	mgr.Lock()
	defer mgr.Unlock()

	mgr.logger.Info("stopping worker")

	mgr.cancel()
	return nil
}

// Perform will accept the job the dispatched it for processing via queue provided
// this will also check for retry count and take action accordingly
func (mgr *Manager) Perform(job IJob, delay time.Duration) error {
	if !mgr.isRegistered(job.GetName()) {
		return ErrorPerformingUnregisteredJob
	}

	if job.GetAttempt() < job.GetMaxRetries() {
		m := &Message{}

		if err := m.Serialize(job); err != nil {
			return err
		}
		_, err := mgr.queue.Enqueue(m.Marshal(), int64(delay.Seconds()), job.GetQueue())
		return err
	}

	return errors.New("maximum attempts exceeded for the job " + job.GetName())
}

// Do fetches a message and process it
// it'll create a job out of received message and performed the lifecycle of it
// also handles retries and acknowledging the message
func (mgr *Manager) Do() error {

	// This constructDetails basically returns the IJob and error
	constructor := mgr.handlers[mgr.identifier]

	// if the queueName is not provided then
	// the default worker name is used as the queueName
	queueName := func() string {
		// If the QueueName is defined by the Job then use that QueueName
		if constructor.QueueName != "" {
			return constructor.QueueName
		}

		return mgr.config.Name
	}()

	job, id, err := mgr.getJob(queueName)

	if job != nil {
		// if the job is not registered one then fail it
		if !mgr.isRegistered(job.GetName()) {
			err = errors.New(job.GetName() + " was is not registered")
			mgr.logger.Error(err.Error())
		} else if err = mgr.timeoutHandler(job); err != nil {
			// In case of any failure while handling the job
			// request the message to perform again
			// with a delay before the message is picked for processing
			if e := mgr.Perform(job, mgr.config.RetryDelay); e != nil {
				mgr.logger.Error("failed to request the job: " + e.Error())
			}
		}

		// Once the execution is successful
		// send and acknowledgement to remove the message from queue
		// in case of error in job its enqueue again to the queue
		// so we delete the message after every receive
		if e := mgr.queue.Acknowledge(id, queueName); e != nil {
			mgr.logger.Error("failed to acknowledge: " + e.Error())
		}

	} else if err != nil {
		mgr.logger.Error("failed to construct job " + err.Error())
	}

	return err
}

// timeoutHandler manages the runtime of the job
// if the job exceeds the timeout then throw an error
func (mgr *Manager) timeoutHandler(job IJob) error {
	var (
		err  error
		done = make(chan error, 1)
	)

	go func() {
		done <- mgr.handle(job)
	}()

	select {
	case <-time.After(job.GetTimeout()):
		err = ErrorJobTimeout
	case err = <-done:
	}

	return err
}

// handle executes the job operation
// and performed it based on job lifecycle defined
func (mgr *Manager) handle(job IJob) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf(job.GetName()+" job failed with error ", r)
		}

		if err != nil {
			mgr.logger.Error(err.Error())
			job.OnError(mgr.ctx, err)
		} else {
			mgr.logger.Debug(job.GetName() + " job processed")
			job.OnSuccess(mgr.ctx)
		}
	}()

	mgr.logger.Debug("processing job " + job.GetName())

	job.IncrementAttempt()
	ctx := job.BeforeHandle(mgr.ctx)

	err = job.Handle(ctx)

	return
}

// getJob constructs job interface based on the message received
func (mgr *Manager) getJob(queueName string) (IJob, string, error) {

	// fetch a message from queue
	id, msg, err := mgr.queue.Dequeue(queueName)
	if err != nil {
		mgr.logger.Error("failed to fetch message from queue: " + err.Error())
		return nil, "", err
	}

	if msg == "" {
		return nil, "", ErrorNoMessageReceived
	}

	// deserialize the message to message struct
	m := &Message{}
	if err := m.Load(msg); err != nil {
		mgr.logger.Error("failed to deserialize message: " + err.Error())
		return nil, "", err
	}

	// construct job from message
	job, err := m.GetJob(mgr.handlers[mgr.identifier])

	if err != nil {
		mgr.logger.Error("failed to construct job from message: " + err.Error())
		return nil, msg, err
	}

	return job, id, err
}

// work handles the continues run of worker
// in case there is no message in the queue it wait for defined time before polling again
// in case of termination it'll wait till the current job to finish
func (mgr *Manager) work() {
	defer mgr.wg.Done()

	// run continually until stop signal is received
	for {
		select {
		// if cancel signal is received then break the loop
		case <-mgr.ctx.Done():
			mgr.logger.Debug("stopping worker")
			return
		default:
			if err := mgr.Do(); err == ErrorNoMessageReceived {
				// if no message is received from last poll
				// wait for some time defined by config.WaitTime
				// or until context is canceled (which ever occurs first)
				// before polling for new message/terminating the worker
				select {
				case <-time.After(mgr.config.WaitTime):
				case <-mgr.ctx.Done():
				}
			}
		}
	}
}

// isRegistered checks if the given job is registered or not
func (mgr *Manager) isRegistered(name string) bool {
	_, ok := mgr.handlers[name]
	return ok
}
