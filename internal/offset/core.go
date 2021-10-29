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
	"runtime/debug"
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

// SetOffset sets/updates aa offset
func (c *Core) SetOffset(ctx context.Context, m *Model) error {
	debug.PrintStack()
	span, ctx := opentracing.StartSpanFromContext(ctx, "OffsetCore.CreateOffset")
	defer span.Finish()

	offsetOperationCount.WithLabelValues(env, "SetOffset").Inc()

	startTime := time.Now()
	defer func() {
		offsetOperationTimeTaken.WithLabelValues(env, "SetOffset").Observe(time.Now().Sub(startTime).Seconds())
	}()
	if m.LatestOffset == "" {
		return merror.Newf(merror.NotFound, "No offset provided for key %s", m.Key())
	}

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
	rollbackOffset := m.rollbackOffset
	if m.LatestOffset != "" {
		m.LatestOffset = newOffsetToSet
		m.rollbackOffset = ""
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
		offsetOperationTimeTaken.WithLabelValues(env, "Get").Observe(time.Now().Sub(startTime).Seconds())
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

// RollbackOffset rolls back to the previous offset in-memory
func (c *Core) RollBackOffset(ctx context.Context, m *Model) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "OffsetCore.RollbackOffset")
	defer span.Finish()
	// Extract offset to avoid being overriden
	verifyOffset := m.LatestOffset
	// This can occur when the offset is set for the first time
	if m.rollbackOffset == "" {
		logger.Ctx(ctx).Errorw("rollback offset key unavailable", "key", m.Key(), "offset", verifyOffset)
		return nil
	}
	offsetOperationCount.WithLabelValues(env, "RollbackOffset").Inc()

	startTime := time.Now()
	defer func() {
		offsetOperationTimeTaken.WithLabelValues(env, "RollbackOffset").Observe(time.Now().Sub(startTime).Seconds())
	}()

	if ok, err := c.Exists(ctx, m); !ok {
		if err != nil {
			return err
		}
		return merror.Newf(merror.NotFound, "offset not found %s", m.Topic)
	}

	m, err := c.GetOffset(ctx, m)
	if err != nil {
		return merror.Newf(merror.NotFound, "failed to rollback offset for key %s", m.Key(), "offset", verifyOffset)
	}
	if m.LatestOffset != verifyOffset {
		return merror.Newf(merror.FailedPrecondition, "Offset no longer valid for key %s", m.Key(), "offset", verifyOffset)
	}

	m.LatestOffset = m.rollbackOffset
	m.rollbackOffset = ""
	return c.repo.Save(ctx, m)
}
