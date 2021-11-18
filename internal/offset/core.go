package offset

////////////////////////////////////////////////////////////////////////////////////
//
// Offset tracking is a in-memory hack to rollback transaction in case of failures in other dependent calls.
// In an ideal state a consumer will contain the latest offset(A)
// If a new offset(B) needs to be committed then we hold A in memory and replace A with B
//                     On commit   [LO->(A), B] => [LO(B)]
//                     On rollback [LO->(B), A] => [LO(A)]
// We hold the rollback offset in memory since rollbacks are local to commit message calls.
//
//////////////////////////////////////////////////////////////////////////////////////
import (
	"context"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/pkg/logger"
)

// ICore is an interface over offset core
type ICore interface {
	SetOffset(ctx context.Context, m *Model) error
	RollBackOffset(ctx context.Context, m *Model) error
	GetOffset(ctx context.Context, m *Model) (*Model, error)
	DeleteOffset(ctx context.Context, m *Model) error
	Exists(ctx context.Context, m *Model) (bool, error)
}

// Core implements all business logic for offset
type Core struct {
	repo IRepo
}

// NewCore returns an instance of Core
func NewCore(repo IRepo) ICore {
	return &Core{repo}
}

// SetOffset sets/updates an offset
func (c *Core) SetOffset(ctx context.Context, m *Model) error {

	span, ctx := opentracing.StartSpanFromContext(ctx, "OffsetCore.CreateOffset")
	defer span.Finish()

	offsetOperationCount.WithLabelValues(env, "SetOffset").Inc()

	startTime := time.Now()
	defer func() {
		offsetOperationTimeTaken.WithLabelValues(env, "SetOffset").Observe(time.Since(startTime).Seconds())
	}()

	ok, err := c.Exists(ctx, m)
	if !ok {
		return c.repo.Save(ctx, m)
	}
	// Extract offset to avoid being overriden
	newOffsetToSet := m.LatestOffset
	m, err = c.GetOffset(ctx, m)
	if err != nil {
		return merror.Newf(merror.NotFound, "failed to update offset")
	}
	// rollbackOffset is private to the OffsetModel and is used to hold the previous offset in memory.
	rollbackOffset := m.rollbackOffset
	if m.LatestOffset != 0 {
		m.LatestOffset = newOffsetToSet
		m.rollbackOffset = 0
	}
	err = c.repo.Save(ctx, m)
	if err != nil {
		return err
	}
	m.rollbackOffset = rollbackOffset
	return nil
}

// Exists to check if the offset exists with fully qualified consul key
func (c *Core) Exists(ctx context.Context, m *Model) (bool, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "OffsetCore.Exists")
	defer span.Finish()

	offsetOperationCount.WithLabelValues(env, "Exists").Inc()

	startTime := time.Now()
	defer func() {
		offsetOperationTimeTaken.WithLabelValues(env, "Exists").Observe(time.Now().Sub(startTime).Seconds())
	}()

	logger.Ctx(ctx).Infow("exists query on offset", "key", m.Key())
	ok, err := c.repo.Exists(ctx, m.Key())
	if err != nil {
		logger.Ctx(ctx).Errorw("error in executing exists", "msg", err.Error())
		return false, err
	}
	return ok, nil
}

// GetOffset returns offset with the given key
func (c *Core) GetOffset(ctx context.Context, m *Model) (*Model, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "OffsetCore.Get")
	defer span.Finish()

	offsetOperationCount.WithLabelValues(env, "Get").Inc()

	startTime := time.Now()
	defer func() {
		offsetOperationTimeTaken.WithLabelValues(env, "Get").Observe(time.Since(startTime).Seconds())
	}()

	prefix := m.Key()
	logger.Ctx(ctx).Infow("fetching offset", "key", prefix)
	err := c.repo.Get(ctx, prefix, m)
	if err != nil {
		logger.Ctx(ctx).Errorw("error fetching offset", "key", m.Key(), "msg", err.Error())
		return nil, err
	}

	return m, nil
}

// RollBackOffset rolls back to the previous offset in-memory
func (c *Core) RollBackOffset(ctx context.Context, m *Model) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "OffsetCore.RollbackOffset")
	defer span.Finish()
	// Extract offset to avoid being overriden
	verifyOffset := m.LatestOffset
	// This can occur when the offset is set for the first time
	if m.rollbackOffset == m.LatestOffset {
		logger.Ctx(ctx).Debugw("rollback offset same as latest offset", "key", m.Key(), "offset", verifyOffset)
		return nil
	}
	offsetOperationCount.WithLabelValues(env, "RollbackOffset").Inc()

	startTime := time.Now()
	defer func() {
		offsetOperationTimeTaken.WithLabelValues(env, "RollbackOffset").Observe(time.Since(startTime).Seconds())
	}()

	if ok, err := c.Exists(ctx, m); !ok {
		if err != nil {
			return err
		}
		return merror.Newf(merror.NotFound, "offset not found %s", m.Topic)
	}

	m, err := c.GetOffset(ctx, m)
	if err != nil {
		return merror.Newf(merror.NotFound, "failed to rollback offset for key %s", m.Key())
	}
	if m.LatestOffset != verifyOffset {
		return merror.Newf(merror.FailedPrecondition, "Offset no longer valid for key %s", m.Key())
	}

	m.LatestOffset = m.rollbackOffset
	m.rollbackOffset = 0
	return c.repo.Save(ctx, m)
}

// DeleteOffset deletes an offset
func (c *Core) DeleteOffset(ctx context.Context, m *Model) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "ProjectCore.DeleteOffset")
	defer span.Finish()

	offsetOperationCount.WithLabelValues(env, "DeleteOffset").Inc()

	startTime := time.Now()
	defer func() {
		offsetOperationTimeTaken.WithLabelValues(env, "DeleteOffset").Observe(time.Since(startTime).Seconds())
	}()

	if ok, err := c.Exists(ctx, m); !ok {
		if err != nil {
			return err
		}
		return merror.Newf(merror.NotFound, "offset not found %s", m.Key())
	}

	return c.repo.Delete(ctx, m)
}
