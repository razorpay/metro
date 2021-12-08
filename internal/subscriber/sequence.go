package subscriber

import (
	"context"

	"github.com/razorpay/metro/internal/offset"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/logger"
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

// OrderingSequenceManager is an interface for managing ordering sequences
type OrderingSequenceManager interface {
	GetOrderedSequenceNum(ctx context.Context, sub *subscription.Model, message messagebroker.ReceivedMessage) (*sequencePair, error)
	SetOrderedSequenceNum(ctx context.Context, sub *subscription.Model, partition int32, orderingKey string, sequenceNum int32) error

	GetLastSequenceStatus(ctx context.Context, sub *subscription.Model, partition int32, orderingKey string) (*lastSequenceStatus, error)
	SetLastSequenceStatus(ctx context.Context, sub *subscription.Model, partition int32, orderingKey string, status *lastSequenceStatus) error

	GetLastMessageSequenceNum(ctx context.Context, sub *subscription.Model, partition int32, orderingKey string) (int32, error)

	DeleteSequence(ctx context.Context, sub *subscription.Model, partition int32, orderingKey string) error
}

type offsetSequenceManager struct {
	offsetCore offset.ICore

	// map of ordering-key -> last read offset
	// keeps track of read but uncommitted message sequences (in flight messages)
	offsetMap map[string]int32
}

// NewOffsetSequenceManager returns an offset based sequence manager
func NewOffsetSequenceManager(ctx context.Context, offsetCore offset.ICore) OrderingSequenceManager {
	return &offsetSequenceManager{offsetCore: offsetCore, offsetMap: make(map[string]int32)}
}

func (o *offsetSequenceManager) GetLastMessageSequenceNum(ctx context.Context, sub *subscription.Model, partition int32, orderingKey string) (int32, error) {
	if val, ok := o.offsetMap[orderingKey]; ok {
		return val, nil
	}

	m := &offset.Model{
		Topic:        sub.Topic,
		Subscription: sub.Name,
		Partition:    partition,
		OrderingKey:  orderingKey,
	}

	m, err := o.offsetCore.GetOffset(ctx, m)
	if err != nil {
		return 0, err
	}

	return m.LatestOffset, nil
}

func (o *offsetSequenceManager) DeleteSequence(ctx context.Context, sub *subscription.Model, partition int32, orderingKey string) error {
	delete(o.offsetMap, orderingKey)
	m := offset.Model{
		Topic:        sub.Topic,
		Subscription: sub.Name,
		Partition:    partition,
		OrderingKey:  orderingKey,
	}
	err := o.offsetCore.DeleteOffset(ctx, &m)
	if err != nil {
		return err
	}
	if err = o.offsetCore.DeleteOffsetStatus(ctx, &offset.Status{Model: m}); err != nil {
		return err
	}
	return nil
}

func (o *offsetSequenceManager) SetOrderedSequenceNum(ctx context.Context, sub *subscription.Model, partition int32, orderingKey string, sequenceNum int32) error {
	m := &offset.Model{
		Topic:        sub.Topic,
		Subscription: sub.Name,
		Partition:    partition,
		OrderingKey:  orderingKey,
		LatestOffset: sequenceNum,
	}

	err := o.offsetCore.SetOffset(ctx, m)
	if err != nil {
		return err
	}

	return nil
}

func (o *offsetSequenceManager) SetLastSequenceStatus(ctx context.Context, sub *subscription.Model, partition int32, orderingKey string, status *lastSequenceStatus) error {
	if status == nil {
		return nil
	}
	os := &offset.Status{
		Model: offset.Model{
			Topic:        sub.Topic,
			Subscription: sub.Name,
			Partition:    partition,
			OrderingKey:  orderingKey,
			LatestOffset: status.SequenceNum,
		},
		OffsetStatus: string(status.Status),
	}

	return o.offsetCore.SetOffsetStatus(ctx, os)
}

// GetLastSequenceStatus returns the last recorded status of an ordering key
func (o *offsetSequenceManager) GetLastSequenceStatus(ctx context.Context, sub *subscription.Model, partition int32, orderingKey string) (*lastSequenceStatus, error) {
	os := &offset.Status{
		Model: offset.Model{
			Topic:        sub.Topic,
			Subscription: sub.Name,
			Partition:    partition,
			OrderingKey:  orderingKey,
		},
	}

	exists, e := o.offsetCore.OffsetStatusExists(ctx, os)
	if e != nil {
		return nil, e
	}

	if !exists {
		return nil, nil
	}

	if os, e = o.offsetCore.GetOffsetStatus(ctx, os); e != nil {
		return nil, e
	}

	return &lastSequenceStatus{
		SequenceNum: os.LatestOffset,
		Status:      sequenceStatus(os.OffsetStatus),
	}, nil
}

// GetOrderedSequenceNum returns sequence number for a message
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
			PrevSequenceNum:    -1,
		}
		o.offsetMap[m.OrderingKey] = s.CurrentSequenceNum
		return s, nil
	}

	off, err := o.offsetCore.GetOffset(ctx, m)
	if err != nil {
		return nil, err
	}

	s := &sequencePair{
		CurrentSequenceNum: message.Offset,
		PrevSequenceNum:    int32(off.LatestOffset),
	}
	if message.Offset < off.LatestOffset {
		// for cases where offset was recorded but not committed to topic
		s = &sequencePair{
			CurrentSequenceNum: message.Offset,
			PrevSequenceNum:    -1,
		}
		logger.Ctx(ctx).Warn(
			"sequenceManager: message with offset lower than recorded offset has appeared",
			"logFields", o.getLogFields(sub),
			"orderingKey", message.OrderingKey,
			"messageOffset", message.Offset,
			"lastOffset", off.LatestOffset,
		)

	}
	o.offsetMap[m.OrderingKey] = s.CurrentSequenceNum
	return s, nil
}

func (o *offsetSequenceManager) getLogFields(sub *subscription.Model) map[string]interface{} {
	return map[string]interface{}{
		"subscription": sub.Name,
		"topic":        sub.Topic,
	}
}
