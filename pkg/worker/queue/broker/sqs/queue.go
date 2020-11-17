package sqs

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"golang.org/x/net/http2"
)

const (
	// Dialect identifier for current dialect
	Dialect = "sqs"
)

// Config holds config for sqs queue
type Config struct {
	QueueName          string
	QueueBatchSize     int32
	Region             string
	Prefix             string
	MaxRetries         int32
	VisibilityTimeout  int64
	WaitTimeout        int64
	CredentialsPath    string
	CredentialsProfile string
	HTTPClient         HTTPClient
}

// HTTPClient holds http client config
type HTTPClient struct {
	ConnectTimeoutMs        int
	ConnKeepAliveMs         int
	ExpectContinueTimeoutMs int
	IdleConnTimeoutMs       int
	MaxAllIdleConns         int
	MaxHostIdleConns        int
	ResponseHeaderTimeoutMs int
	TLSHandshakeTimeoutMs   int
}

// Queue holds the handler for the client and auxiliary info
type Queue struct {
	name              string
	client            *sqs.SQS
	prefix            string
	WaitTimeout       int64
	VisibilityTimeout int64
}

// New inits a queue driver
func New(queueName string, conf *Config) (Queue, error) {
	var q Queue
	s, err := session.NewSession(&aws.Config{
		Region:      aws.String(conf.Region),
		MaxRetries:  aws.Int(int(conf.MaxRetries)),
		HTTPClient:  newHTTPClientWithSettings(conf.HTTPClient),
		Credentials: credentials.NewSharedCredentials(conf.CredentialsPath, conf.CredentialsProfile),
	})

	if err != nil {
		return q, err
	}

	sqsClient := sqs.New(s)

	q = Queue{
		name:   queueName,
		client: sqsClient,
		prefix: conf.Prefix,
	}

	return q, nil
}

// Enqueue adds a message to the queue
func (q Queue) Enqueue(message string, delay int64, queueName string) (string, error) {
	result, err := q.client.SendMessage(
		&sqs.SendMessageInput{
			DelaySeconds: aws.Int64(delay),
			MessageAttributes: map[string]*sqs.MessageAttributeValue{
				"Timestamp": {
					DataType:    aws.String("String"),
					StringValue: aws.String(time.Now().String()),
				},
			},
			MessageBody: aws.String(message),
			QueueUrl:    aws.String(q.getQueueUrl(queueName)),
		})

	if err != nil {
		return "", err
	}

	return *result.MessageId, nil
}

// Dequeue fetches a set of messages from the queue
func (q Queue) Dequeue(queueName string) (string, string, error) {
	result, err := q.client.ReceiveMessage(
		&sqs.ReceiveMessageInput{
			AttributeNames: []*string{
				aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
			},
			MessageAttributeNames: []*string{
				aws.String(sqs.QueueAttributeNameAll),
			},
			QueueUrl:          aws.String(q.getQueueUrl(queueName)),
			VisibilityTimeout: aws.Int64(q.VisibilityTimeout),
			WaitTimeSeconds:   aws.Int64(q.WaitTimeout),
		})

	if err != nil {
		return "", "", err
	}

	if len(result.Messages) == 0 {
		return "", "", errors.New("no messages found")
	}

	msg := result.Messages[0]
	return *msg.ReceiptHandle, *msg.Body, nil
}

func (q Queue) Acknowledge(id string, queueName string) error {
	// Delete from pendingQueue (this ensures that the message is not processed by another worker)
	_, err := q.client.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(q.getQueueUrl(queueName)),
		ReceiptHandle: aws.String(id),
	})

	return err
}

// getQueueUrl constructs the URL based on prefix and queue name
func (q Queue) getQueueUrl(queueName string) string {
	return fmt.Sprintf("%s/%s", strings.TrimRight(q.prefix, "/"), queueName)
}

// newHTTPClientWithSettings creates new HTTP client using given config
// if the config is not provided then creates a client with default settings
func newHTTPClientWithSettings(httpSettings HTTPClient) *http.Client {
	if (httpSettings == HTTPClient{}) {
		return http.DefaultClient
	}

	tr := &http.Transport{
		ResponseHeaderTimeout: time.Duration(httpSettings.ResponseHeaderTimeoutMs) * time.Millisecond,
		Proxy:                 http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			KeepAlive: time.Duration(httpSettings.ConnKeepAliveMs) * time.Millisecond,
			Timeout:   time.Duration(httpSettings.ConnectTimeoutMs) * time.Millisecond,
		}).DialContext,
		MaxIdleConns:          httpSettings.MaxAllIdleConns,
		IdleConnTimeout:       time.Duration(httpSettings.IdleConnTimeoutMs) * time.Millisecond,
		TLSHandshakeTimeout:   time.Duration(httpSettings.TLSHandshakeTimeoutMs) * time.Millisecond,
		MaxIdleConnsPerHost:   httpSettings.MaxHostIdleConns,
		ExpectContinueTimeout: time.Duration(httpSettings.ExpectContinueTimeoutMs) * time.Millisecond,
	}

	// So client makes HTTP/2 requests
	_ = http2.ConfigureTransport(tr)

	return &http.Client{
		Transport: tr,
	}
}
