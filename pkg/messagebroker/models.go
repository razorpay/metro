package messagebroker

import (
	"time"
)

// request models for messagebroker
type CreateTopicRequest struct {
	Name          string
	NumPartitions int
}

type DeleteTopicRequest struct {
	Name string
}

type SendMessageToTopicRequest struct {
	Topic   string
	Message []byte
}

type GetMessagesFromTopicRequest struct {
	NumOfMessages int
	Timeout       time.Duration
}

// response models for messagebroker
type IResponse interface {
	HasError() bool
	GetError() error
	GetResponse() interface{}
}

type BaseResponse struct {
	Error    error
	Response interface{}
}

func (r *BaseResponse) HasError() bool {
	return r.Error != nil
}

func (r *BaseResponse) GetError() error {
	return r.Error
}

func (r *BaseResponse) GetResponse() interface{} {
	return r.Response
}

type CreateTopicResponse struct {
	BaseResponse
}

type DeleteTopicResponse struct {
	BaseResponse
}

type SendMessageToTopicResponse struct {
	BaseResponse
	MessageId string
}

type GetMessagesFromTopicResponse struct {
	BaseResponse
	Messages []string
}
