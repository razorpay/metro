package worker_test

import (
	"context"
	"encoding/gob"
	"errors"
	"testing"
	"time"

	"bou.ke/monkey"
	"github.com/golang/mock/gomock"
	"github.com/razorpay/metro/pkg/worker"
	"github.com/razorpay/metro/pkg/worker/mock/logger"
	queueMock "github.com/razorpay/metro/pkg/worker/mock/queue"
	"github.com/stretchr/testify/assert"
)

func TestManager_Register(t *testing.T) {
	tests := []struct {
		name        string
		job         worker.IJob
		constructor worker.Constructor
		err         error
	}{
		{
			name:        "register",
			job:         &Job{worker.Job{Name: "testCase"}},
			err:         nil,
			constructor: nil,
		},
		{
			name:        "duplicate register",
			job:         &TestJob{worker.Job{Name: "testCase"}},
			err:         errors.New("handler already mapped for name testCase"),
			constructor: nil,
		},
		{
			name:        "register with constructor",
			job:         &RawJob{Job: worker.Job{Name: "raw_job"}},
			err:         nil,
			constructor: (&RawJob{}).Construct,
		},
	}
	w := worker.NewManager(&worker.Config{}, nil, nil)

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			err := w.Register(testCase.job, testCase.constructor)
			assert.Equal(t, testCase.err, err)
		})
	}
}

func TestManager_Perform(t *testing.T) {

	tests := []struct {
		name  string
		mock  func(worker.IManager, *queueMock.MockIQueue)
		job   worker.IJob
		delay time.Duration
		err   error
	}{
		{
			name: "max attempt",
			mock: func(manager worker.IManager, queue *queueMock.MockIQueue) {
				_ = manager.Register(&Job{
					worker.Job{Name: "job"},
				})
			},
			job:   &Job{worker.Job{Name: "job"}},
			delay: 0,
			err:   errors.New("maximum attempts exceeded for the job job"),
		},
		{
			name:  "unregistered job",
			mock:  func(manager worker.IManager, queue *queueMock.MockIQueue) {},
			job:   &TestJob{worker.Job{Name: "test_job"}},
			delay: 0,
			err:   worker.ErrorPerformingUnregisteredJob,
		},
		{
			name: "serialization failure",
			mock: func(manager worker.IManager, queue *queueMock.MockIQueue) {
				pg := monkey.Patch(gob.Register, func(interface{}) {})
				defer pg.Unpatch()

				_ = manager.Register(&UnregisteredJob{
					worker.Job{Name: "job"},
				})
			},
			job:   &UnregisteredJob{worker.Job{Name: "job", MaxRetries: 1}},
			delay: 0,
			err:   errors.New("serialization failed gob: type not registered for interface: worker_test.UnregisteredJob"),
		},
		{
			name: "queue failure",
			mock: func(manager worker.IManager, queue *queueMock.MockIQueue) {
				_ = manager.Register(&Job{
					worker.Job{Name: "job", QueueName: "temp_queue"},
				})

				queue.
					EXPECT().
					Enqueue(`{"serialized":true,"content":"LRAAECp3b3JrZXJfdGVzdC5Kb2L/gQMBAQNKb2IB/4IAAQEBA0pvYgH/hAAAAE//gwMBAQNKb2IB/4QAAQUBCVF1ZXVlTmFtZQEMAAEETmFtZQEMAAEKTWF4UmV0cmllcwEEAAEHQXR0ZW1wdAEEAAEHVGltZW91dAEEAAAAGf+CFgEBCnRlbXBfcXVldWUBA2pvYgECAAA="}`, int64(0), "temp_queue").
					Return("something", errors.New("failed to enqueue message"))
			},
			job:   &Job{worker.Job{Name: "job", MaxRetries: 1, QueueName: "temp_queue"}},
			delay: 0,
			err:   errors.New("failed to enqueue message"),
		},
		{
			name: "success",
			mock: func(manager worker.IManager, queue *queueMock.MockIQueue) {
				_ = manager.Register(&Job{
					worker.Job{Name: "job", QueueName: "temp_queue"},
				})

				queue.
					EXPECT().
					Enqueue(`{"serialized":true,"content":"LRAAECp3b3JrZXJfdGVzdC5Kb2L/gQMBAQNKb2IB/4IAAQEBA0pvYgH/hAAAAE//gwMBAQNKb2IB/4QAAQUBCVF1ZXVlTmFtZQEMAAEETmFtZQEMAAEKTWF4UmV0cmllcwEEAAEHQXR0ZW1wdAEEAAEHVGltZW91dAEEAAAAGf+CFgEBCnRlbXBfcXVldWUBA2pvYgECAAA="}`, int64(0), "temp_queue").
					Return("something", nil)
			},
			job:   &Job{worker.Job{Name: "job", MaxRetries: 1, QueueName: "temp_queue"}},
			delay: 0,
			err:   nil,
		},
		{
			name: "success",
			mock: func(manager worker.IManager, queue *queueMock.MockIQueue) {
				_ = manager.Register(&Job{
					worker.Job{Name: "job", QueueName: "temp_queue"},
				})

				queue.
					EXPECT().
					Enqueue(`{"serialized":true,"content":"LRAAECp3b3JrZXJfdGVzdC5Kb2L/gQMBAQNKb2IB/4IAAQEBA0pvYgH/hAAAAE//gwMBAQNKb2IB/4QAAQUBCVF1ZXVlTmFtZQEMAAEETmFtZQEMAAEKTWF4UmV0cmllcwEEAAEHQXR0ZW1wdAEEAAEHVGltZW91dAEEAAAAGf+CFgEBCnRlbXBfcXVldWUBA2pvYgECAAA="}`, int64(0), "temp_queue").
					Return("something", nil)
			},
			job:   &Job{worker.Job{Name: "job", MaxRetries: 1, QueueName: "temp_queue"}},
			delay: 0,
			err:   nil,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			queue := queueMock.NewMockIQueue(ctrl)
			w := worker.NewManager(&worker.Config{}, queue, getLogger(ctrl))
			testCase.mock(w, queue)

			err := w.Perform(testCase.job, testCase.delay)

			assert.Equal(t, testCase.err, err)
		})
	}
}

func TestManager_Start_Failure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	queue := queueMock.NewMockIQueue(ctrl)
	w := worker.NewManager(&worker.Config{}, queue, getLogger(ctrl))

	err := w.Start(context.TODO())
	assert.Equal(t, worker.ErrorStartingUnregisteredJob, err)
}

func TestManager_Start(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	queue := queueMock.NewMockIQueue(ctrl)
	w := worker.NewManager(&worker.Config{Name: "something", WaitTime: 50 * time.Millisecond}, queue, getLogger(ctrl))

	_ = w.Register(&Job{worker.Job{Name: "something", QueueName: "temp_queue"}}, (&RawJob{}).Construct)

	queue.
		EXPECT().
		Dequeue("temp_queue").
		Return("", "", errors.New("not found")).
		AnyTimes()

	var err error

	go func() {
		<-time.Tick(1 * time.Second)

		err = w.Stop()
		assert.Nil(t, err)
	}()

	err = w.Start(context.TODO())
	assert.Nil(t, err)
}

func TestNewManager_Termination(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	queue := queueMock.NewMockIQueue(ctrl)

	tests := []struct {
		name       string
		mock       func()
		stopError  error
		startError error
		stopDelay  time.Duration
		minRuntime time.Duration
		maxRuntime time.Duration
		job        worker.IJob
		waitTime   time.Duration
	}{
		{
			name: "delayed job",
			mock: func() {
				queue.
					EXPECT().
					Dequeue("delayed_job").
					Return(
						"1123",
						`{"serialized":true,"content":"OxAAFyp3b3JrZXJfdGVzdC5EZWxheWVkSm9i/4EDAQEKRGVsYXllZEpvYgH/ggABAQEDSm9iAf+EAAAAVf+DAwEBA0pvYgH/hAABBgEFUXVldWUBDAABBEFyZ3MB/4YAAQROYW1lAQwAAQpNYXhSZXRyaWVzAQQAAQdBdHRlbXB0AQQAAQdUaW1lb3V0AQQAAAAU/4UEAQEEQXJncwH/hgABDAEQAAAG/4IDAQAA"}`,
						nil)

				queue.
					EXPECT().
					Acknowledge("1123", "delayed_job").
					Return(nil)
			},
			startError: nil,
			stopError:  nil,
			minRuntime: 3 * time.Second,
			maxRuntime: 4 * time.Second,
			stopDelay:  1 * time.Second,
			job:        &DelayedJob{worker.Job{Name: "delayed_job"}},
			waitTime:   30,
		},
		{
			name: "failed job",
			mock: func() {
				queue.
					EXPECT().
					Dequeue("failure_job").
					Return("",
						"",
						nil)
			},
			startError: nil,
			stopError:  nil,
			minRuntime: 1 * time.Second,
			maxRuntime: 2 * time.Second,
			stopDelay:  1 * time.Second,
			job:        &FailureJob{worker.Job{Name: "failure_job"}},
			waitTime:   30,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			w := worker.NewManager(
				&worker.Config{
					Name:     testCase.job.GetName(),
					WaitTime: testCase.waitTime * time.Second,
				}, queue, getLogger(ctrl))

			_ = w.Register(testCase.job)

			testCase.mock()

			var err error
			go func() {
				<-time.After(testCase.stopDelay)

				err = w.Stop()
				assert.Equal(t, testCase.stopError, err)
			}()

			st := time.Now()
			err = w.Start(context.TODO())
			assert.Equal(t, testCase.startError, err)
			assert.Less(t, time.Since(st).Seconds(), testCase.maxRuntime.Seconds())
			assert.Greater(t, time.Since(st).Seconds(), testCase.minRuntime.Seconds())
		})
	}
}

func TestManager_Do(t *testing.T) {
	tests := []struct {
		name        string
		mock        func(*queueMock.MockIQueue)
		err         error
		job         worker.IJob
		constructor worker.Constructor
	}{
		{
			name: "failed to fetch message from queue",
			mock: func(queue *queueMock.MockIQueue) {
				queue.
					EXPECT().
					Dequeue("job").
					Return("",
						"",
						errors.New("failed to receive message"))
			},
			job: &Job{worker.Job{Name: "job"}},
			err: errors.New("failed to receive message"),
		},
		{
			name: "empty message",
			mock: func(queue *queueMock.MockIQueue) {
				queue.
					EXPECT().
					Dequeue("job").
					Return("", "", nil)
			},
			job: &Job{worker.Job{Name: "job"}},
			err: worker.ErrorNoMessageReceived,
		},
		{
			name: "message load failure",
			mock: func(queue *queueMock.MockIQueue) {
				queue.
					EXPECT().
					Dequeue("job").
					Return("123", `{"serialized":true}`, nil)
			},
			job: &Job{worker.Job{Name: "job"}},
			err: errors.New("failed to assert data type of serialized message"),
		},
		{
			name: "job construction failure",
			mock: func(queue *queueMock.MockIQueue) {
				queue.
					EXPECT().
					Dequeue("job").
					Return("123", `{"serialized": true,"content":"NRAAFCp3b3JrZXJfdGVzdC5UZXN0Sm9i/4cDAQEHVGVzdEpvYgH/iAABAQEDSm9iAf+EAAAAVf+DAwEBA0pvYgH/hAABBgEFUXVldWUBDAABBEFyZ3MB/4YAAQROYW1lAQwAAQpNYXhSZXRyaWVzAQQAAQdBdHRlbXB0AQQAAQdUaW1lb3V0AQQAAAAU/4UEAQEEQXJncwH/hgABDAEQAAAG/4gDAQAA"}`, nil)
			},
			job: &Job{worker.Job{Name: "job"}},
			err: errors.New(`failed to deserialize gob: name not registered for interface: "*worker_test.TestJob"`),
		},
		{
			name: "job failure exhaust retry",
			mock: func(queue *queueMock.MockIQueue) {
				queue.
					EXPECT().
					Dequeue("failure_job").
					Return("123", `{"serialized":true,"content":"OxAAFyp3b3JrZXJfdGVzdC5GYWlsdXJlSm9i/4cDAQEKRmFpbHVyZUpvYgH/iAABAQEDSm9iAf+EAAAAVf+DAwEBA0pvYgH/hAABBgEFUXVldWUBDAABBEFyZ3MB/4YAAQROYW1lAQwAAQpNYXhSZXRyaWVzAQQAAQdBdHRlbXB0AQQAAQdUaW1lb3V0AQQAAAAU/4UEAQEEQXJncwH/hgABDAEQAAAT/4gQAQMLZmFpbHVyZV9qb2IAAA=="}`, nil)

				queue.
					EXPECT().
					Acknowledge("123", "failure_job").
					Return(nil)
			},
			job: &FailureJob{worker.Job{Name: "failure_job"}},
			err: errors.New("failed to complete job"),
		},
		{
			name: "received unregistered job",
			mock: func(queue *queueMock.MockIQueue) {
				queue.
					EXPECT().
					Dequeue("job").
					Return("123", `{"serialized": true,"content":"OxAAFyp3b3JrZXJfdGVzdC5GYWlsdXJlSm9i/4cDAQEKRmFpbHVyZUpvYgH/iAABAQEDSm9iAf+EAAAAVf+DAwEBA0pvYgH/hAABBgEFUXVldWUBDAABBEFyZ3MB/4YAAQROYW1lAQwAAQpNYXhSZXRyaWVzAQQAAQdBdHRlbXB0AQQAAQdUaW1lb3V0AQQAAAAU/4UEAQEEQXJncwH/hgABDAEQAAAT/4gQAQMLZmFpbHVyZV9qb2IAAA=="}`, nil)

				queue.
					EXPECT().
					Acknowledge("123", "job").
					Return(nil)
			},
			job: &FailureJob{worker.Job{Name: "job"}},
			err: errors.New("failure_job was is not registered"),
		},
		{
			name: "job successful",
			mock: func(queue *queueMock.MockIQueue) {
				queue.
					EXPECT().
					Dequeue("test_job").
					Return("123", `{"serialized": true,"content":"NRAAFCp3b3JrZXJfdGVzdC5UZXN0Sm9i/4EDAQEHVGVzdEpvYgH/ggABAQEDSm9iAf+EAAAAVf+DAwEBA0pvYgH/hAABBgEFUXVldWUBDAABBEFyZ3MB/4YAAQROYW1lAQwAAQpNYXhSZXRyaWVzAQQAAQdBdHRlbXB0AQQAAQdUaW1lb3V0AQQAAAAU/4UEAQEEQXJncwH/hgABDAEQAAAQ/4INAQMIdGVzdF9qb2IAAA=="}`, nil)

				queue.
					EXPECT().
					Acknowledge("123", "test_job").
					Return(nil)
			},
			job: &TestJob{worker.Job{Name: "test_job"}},
			err: nil,
		},
		{
			name: "job successful ack failure",
			mock: func(queue *queueMock.MockIQueue) {
				queue.
					EXPECT().
					Dequeue("test_job").
					Return("123", `{"serialized": true,"content":"NRAAFCp3b3JrZXJfdGVzdC5UZXN0Sm9i/4EDAQEHVGVzdEpvYgH/ggABAQEDSm9iAf+EAAAAVf+DAwEBA0pvYgH/hAABBgEFUXVldWUBDAABBEFyZ3MB/4YAAQROYW1lAQwAAQpNYXhSZXRyaWVzAQQAAQdBdHRlbXB0AQQAAQdUaW1lb3V0AQQAAAAU/4UEAQEEQXJncwH/hgABDAEQAAAQ/4INAQMIdGVzdF9qb2IAAA=="}`, nil)

				queue.
					EXPECT().
					Acknowledge("123", "test_job").
					Return(errors.New("ack failed"))
			},
			job: &TestJob{worker.Job{Name: "test_job"}},
			err: nil,
		},
		{
			name: "invalid job received",
			mock: func(queue *queueMock.MockIQueue) {
				queue.
					EXPECT().
					Dequeue("test_job").
					Return("123", `{"serialized": true,"content":"NRAAFCp3b3JrZXJfdGVzdC5UZXN0Sm9i/4EDAQEHVGVzdEpvYgH/ggABAQEDSm9iAf+EAAAAVf+DAwEBA0pvYgH/hAABBgEFUXVldWUBDAABBEFyZ3MB/4YAAQROYW1lAQwAAQpNYXhSZXRyaWVzAQQAAQdBdHRlbXB0AQQAAQdUaW1lb3V0AQQAAAAU/4UEAQEEQXJncwH/hgABDAEQAAAQ/4INAQMIdGVzdF9qb2IAAA=="}`, nil)

				queue.
					EXPECT().
					Acknowledge("123", "test_job").
					Return(nil)
			},
			job: &TestJob{worker.Job{Name: "test_job"}},
		},
		{
			name: "job panic",
			mock: func(queue *queueMock.MockIQueue) {
				queue.
					EXPECT().
					Dequeue(gomock.Any()).
					Return("123", `{"serialized": true,"content":"LRAAECp3b3JrZXJfdGVzdC5Kb2L/gQMBAQNKb2IB/4IAAQEBA0pvYgH/hAAAAFX/gwMBAQNKb2IB/4QAAQYBBVF1ZXVlAQwAAQRBcmdzAf+GAAEETmFtZQEMAAEKTWF4UmV0cmllcwEEAAEHQXR0ZW1wdAEEAAEHVGltZW91dAEEAAAAFP+FBAEBBEFyZ3MB/4YAAQwBEAAABv+CAwEAAA=="}`, nil)

				queue.
					EXPECT().
					Acknowledge("123", "default").
					Return(nil)
			},
			job: &Job{worker.Job{}},
			err: errors.New(" job failed with error %!(EXTRA string=handle method is not implemented)"),
		},
		{
			name: "job timeout",
			mock: func(queue *queueMock.MockIQueue) {
				queue.
					EXPECT().
					Dequeue("timeout_job").
					Return("123", `{"serialized": true,"content":"OxAAFyp3b3JrZXJfdGVzdC5UaW1lb3V0Sm9i/4EDAQEKVGltZW91dEpvYgH/ggABAQEDSm9iAf+EAAAAS/+DAwEBA0pvYgH/hAABBQEFUXVldWUBDAABBE5hbWUBDAABCk1heFJldHJpZXMBBAABB0F0dGVtcHQBBAABB1RpbWVvdXQBBAAAABv/ghgBAgt0aW1lb3V0X2pvYgEEAvw7msoAAAA="}`, nil)

				queue.
					EXPECT().
					Enqueue(gomock.Any(), gomock.Any(), gomock.Any()).
					Return("", nil)

				queue.
					EXPECT().
					Acknowledge("123", "timeout_job").
					Return(nil)
			},
			job: &TimeoutJob{worker.Job{Name: "timeout_job"}},
			err: worker.ErrorJobTimeout,
		},
		{
			name: "raw job when queueName is passed",
			mock: func(queue *queueMock.MockIQueue) {
				queue.
					EXPECT().
					Dequeue("temp_queue").
					Return("123", `{"some":"string"}`, nil)

				queue.
					EXPECT().
					Acknowledge(gomock.Any(), "temp_queue").
					Return(nil)
			},
			job:         &RawJob{Job: worker.Job{Name: "raw_job", QueueName: "temp_queue"}},
			constructor: (&RawJob{}).Construct,
			err:         nil,
		},
		{
			name: "raw job when queueName is not passed",
			mock: func(queue *queueMock.MockIQueue) {
				queue.
					EXPECT().
					Dequeue("raw_job").
					Return("123", `{"some":"string"}`, nil)

				queue.
					EXPECT().
					Acknowledge(gomock.Any(), "raw_job").
					Return(nil)
			},
			job:         &RawJob{Job: worker.Job{Name: "raw_job"}},
			constructor: (&RawJob{}).Construct,
			err:         nil,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			queue := queueMock.NewMockIQueue(ctrl)
			w := worker.NewManager(
				&worker.Config{
					Name:     testCase.job.GetName(),
					WaitTime: 5 * time.Second,
				}, queue, getLogger(ctrl))

			testCase.mock(queue)
			_ = w.Register(testCase.job, testCase.constructor)
			assert.Equal(t, testCase.err, w.Do())
		})
	}
}

func TestManager_DelayedPolling(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	queue := queueMock.NewMockIQueue(ctrl)
	w := worker.NewManager(
		&worker.Config{
			Name:     "something",
			WaitTime: 2 * time.Second,
		}, queue, getLogger(ctrl))

	_ = w.Register(&Job{worker.Job{Name: "something"}})

	queue.
		EXPECT().
		Dequeue("something").
		Return("", "", nil).
		Times(2)

	var err error

	go func() {
		<-time.Tick(3 * time.Second)

		err = w.Stop()
		assert.Nil(t, err)
	}()

	err = w.Start(context.TODO())
	assert.Nil(t, err)
}

func getLogger(ctrl *gomock.Controller) worker.ILogger {
	log := logger.NewMockILogger(ctrl)

	log.
		EXPECT().
		Error(gomock.Any()).
		AnyTimes()

	log.
		EXPECT().
		Debug(gomock.Any()).
		AnyTimes()

	log.
		EXPECT().
		Info(gomock.Any()).
		AnyTimes()

	return log
}
