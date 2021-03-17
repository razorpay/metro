package subscription

import (
	"context"

	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/internal/project"
	"github.com/razorpay/metro/internal/topic"
	"github.com/razorpay/metro/pkg/logger"
)

// ICore is an interface over subscription core
type ICore interface {
	CreateSubscription(ctx context.Context, subscription *Model) error
	Exists(ctx context.Context, key string) (bool, error)
	DeleteSubscription(ctx context.Context, m *Model) error
	DeleteProjectSubscriptions(ctx context.Context, projectID string) error
	GetTopicFromSubscriptionName(ctx context.Context, subscription string) (string, error)
	ListKeys(ctx context.Context, prefix string) ([]string, error)
	List(ctx context.Context, prefix string) ([]*Model, error)
	Get(ctx context.Context, key string) (*Model, error)
}

// Core implements all business logic for a subscription
type Core struct {
	repo        IRepo
	projectCore project.ICore
	topicCore   topic.ICore
}

// NewCore returns an instance of Core
func NewCore(repo IRepo, projectCore project.ICore, topicCore topic.ICore) *Core {
	return &Core{repo, projectCore, topicCore}
}

// CreateSubscription creates a subscription for a given topic
func (c *Core) CreateSubscription(ctx context.Context, m *Model) error {
	// the order of checks which google pub/sub does
	// 1. check if subscription project exists
	// 2. check if subscription exists
	// 3. check if topic project exists
	// 4. check if topic exists
	if ok, err := c.projectCore.ExistsWithID(ctx, m.ExtractedSubscriptionProjectID); !ok {
		if err != nil {
			return err
		}
		logger.Ctx(ctx).Errorw("subscription project not found", "name", m.ExtractedSubscriptionProjectID)
		return merror.New(merror.NotFound, "project not found")
	}
	ok, err := c.Exists(ctx, m.Key())
	if err != nil {
		return err
	}
	if ok {
		return merror.Newf(merror.AlreadyExists, "subscription with id %s already exists", m.Name)
	}
	if ok, err := c.projectCore.ExistsWithID(ctx, m.ExtractedTopicProjectID); !ok {
		if err != nil {
			return err
		}
		logger.Ctx(ctx).Errorw("topic project not found", "name", m.ExtractedTopicProjectID)
		return merror.New(merror.NotFound, "project not found")
	}
	if ok, err = c.topicCore.ExistsWithName(ctx, m.Topic); !ok {
		if err != nil {
			return err
		}
		return merror.Newf(merror.NotFound, "topic with name %s not found", m.Topic)
	}
	return c.repo.Create(ctx, m)
}

// Exists checks if subscription exists for a given key
func (c *Core) Exists(ctx context.Context, key string) (bool, error) {
	ok, err := c.repo.Exists(ctx, key)
	if err != nil {
		logger.Ctx(ctx).Errorw("error in executing exists", "msg", err.Error(), "key", key)
		return false, err
	}
	return ok, nil
}

// DeleteSubscription deletes a subscription
func (c *Core) DeleteSubscription(ctx context.Context, m *Model) error {
	if ok, err := c.projectCore.ExistsWithID(ctx, m.ExtractedSubscriptionProjectID); !ok {
		if err != nil {
			return err
		}
		return merror.Newf(merror.NotFound, "project not found")
	}
	if ok, err := c.Exists(ctx, m.Key()); !ok {
		if err != nil {
			return err
		}
		return merror.Newf(merror.NotFound, "Subscription does not exist")
	}
	return c.repo.Delete(ctx, m)
}

// DeleteProjectSubscriptions deletes all subscriptions for the given projectID
func (c *Core) DeleteProjectSubscriptions(ctx context.Context, projectID string) error {
	if projectID == "" {
		return merror.Newf(merror.InvalidArgument, "invalid projectID: %s", projectID)
	}

	prefix := common.BasePrefix + Prefix + projectID

	return c.repo.DeleteTree(ctx, prefix)
}

// GetTopicFromSubscriptionName returns topic from subscription
func (c *Core) GetTopicFromSubscriptionName(ctx context.Context, subscription string) (string, error) {
	projectID, subscriptionName, err := extractSubscriptionMetaAndValidate(ctx, subscription)
	if err != nil {
		return "", err
	}
	subscriptionKey := common.BasePrefix + Prefix + projectID + "/" + subscriptionName

	if ok, err := c.repo.Exists(ctx, subscriptionKey); !ok {
		if err != nil {
			return "", err
		}
		err = merror.Newf(merror.NotFound, "subscription not found %s", common.BasePrefix+subscription)
		logger.Ctx(ctx).Error(err.Error())
		return "", err
	}
	m := &Model{}
	err = c.repo.Get(ctx, subscriptionKey, m)
	if err != nil {
		return "", err
	}
	return m.Topic, nil
}

// ListKeys gets all subscription keys
func (c *Core) ListKeys(ctx context.Context, prefix string) ([]string, error) {
	prefix = Prefix + prefix
	return c.repo.ListKeys(ctx, prefix)
}

// List gets slice of subscriptions starting with given prefix
func (c *Core) List(ctx context.Context, prefix string) ([]*Model, error) {
	prefix = Prefix + prefix

	out := []*Model{}
	ret, err := c.repo.List(ctx, prefix)
	if err != nil {
		return nil, err
	}

	for _, obj := range ret {
		out = append(out, obj.(*Model))
	}
	return out, nil
}

// Get returns subscription with the given key
func (c *Core) Get(ctx context.Context, key string) (*Model, error) {
	projectID, subscriptionName, err := extractSubscriptionMetaAndValidate(ctx, key)
	if err != nil {
		return nil, err
	}
	prefix := common.BasePrefix + Prefix + projectID + "/" + subscriptionName

	model := &Model{}
	err = c.repo.Get(ctx, prefix, model)
	if err != nil {
		return nil, err
	}
	return model, nil
}
