package subscriber

import (
	"context"
	"strconv"

	"github.com/razorpay/metro/internal/offset"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/messagebroker"
)

type sequenceStatus string

const (
	sequenceSuccess sequenceStatus = "SUCCESS"
	sequenceFailure sequenceStatus = "FAILURE"
	sequenceDLQ     sequenceStatus = "DLQ"
)

type sequencePair struct {
	CurrentSequenceNum int32
	PrevSequenceNum    int32
}

type lastSequenceStatus struct {
	SequenceNum int32
	Status      sequenceStatus
}

type orderingSequenceManager interface {
	GetOrderedSequenceNum(ctx context.Context, sub *subscription.Model, message messagebroker.ReceivedMessage) (*sequencePair, error)
	GetLastSequenceStatus(ctx context.Context, sub *subscription.Model, partition int32, orderingKey string) (*lastSequenceStatus, error)
}

type offsetSequenceManager struct {
	offsetCore offset.ICore
	offsetMap  map[string]int32 // map of ordering-key -> last read offset
}

func NewOffsetSequenceManager(ctx context.Context, offsetCore offset.ICore) orderingSequenceManager {
	return &offsetSequenceManager{offsetCore: offsetCore, offsetMap: make(map[string]int32)}
}

func (o *offsetSequenceManager) GetLastSequenceStatus(ctx context.Context, sub *subscription.Model, partition int32, orderingKey string) (*lastSequenceStatus, error) {
	return nil, nil
}

func (o *offsetSequenceManager) GetOrderedSequenceNum(ctx context.Context, sub *subscription.Model, message messagebroker.ReceivedMessage) (*sequencePair, error) {
	// Check in the local map first
	if offset, ok := o.offsetMap[message.OrderingKey]; ok {
		s := &sequencePair{
			CurrentSequenceNum: message.Offset,
			PrevSequenceNum:    offset,
		}
		o.offsetMap[message.OrderingKey] = s.CurrentSequenceNum
		return s, nil
	}

	// Check in datastore
	m := &offset.Model{
		Topic:        sub.Topic,
		Subscription: sub.Name,
		Partition:    message.Partition,
		OrderingKey:  message.OrderingKey,
	}

	ok, err := o.offsetCore.Exists(ctx, m)
	if err != nil {
		return nil, err
	}

	if !ok {
		// If first message in order group
		s := &sequencePair{
			CurrentSequenceNum: message.Offset,
			PrevSequenceNum:    message.Offset,
		}
		o.offsetMap[m.OrderingKey] = s.CurrentSequenceNum
		return s, nil
	}

	off, err := o.offsetCore.GetOffset(ctx, m)
	if err != nil {
		return nil, err
	}

	lastOffset, err := strconv.Atoi(off.LatestOffset)
	if err != nil {
		return nil, err
	}

	s := &sequencePair{
		CurrentSequenceNum: message.Offset,
		PrevSequenceNum:    int32(lastOffset),
	}
	o.offsetMap[m.OrderingKey] = s.CurrentSequenceNum
	return s, nil
}
