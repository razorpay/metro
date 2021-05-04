package topic

import (
	"context"
	"time"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/internal/project"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
)

// ICore is an interface over topic core
type ICore interface {
	CreateTopic(ctx context.Context, model *Model) error
	Exists(ctx context.Context, key string) (bool, error)
	ExistsWithName(ctx context.Context, name string) (bool, error)
	DeleteTopic(ctx context.Context, m *Model) error
	DeleteProjectTopics(ctx context.Context, projectID string) error
	Get(ctx context.Context, key string) (*Model, error)
	CreateRetryTopic(ctx context.Context, model *Model) error
	CreateDeadLetterTopic(ctx context.Context, model *Model) error
}

// Core implements all business logic for a topic
type Core struct {
	repo        IRepo
	projectCore project.ICore
	brokerStore brokerstore.IBrokerStore
}

// NewCore returns an instance of Core
func NewCore(repo IRepo, projectCore project.ICore, brokerStore brokerstore.IBrokerStore) *Core {
	return &Core{repo, projectCore, brokerStore}
}

// CreateTopic implements topic creation
func (c *Core) CreateTopic(ctx context.Context, m *Model) error {
	topicOperationCount.WithLabelValues(env, "CreateTopic").Inc()

	startTime := time.Now()
	defer topicOperationTimeTaken.WithLabelValues(env, "CreateTopic").Observe(float64(time.Since(startTime).Nanoseconds() / 1e9))

	// validate project exists
	if ok, err := c.projectCore.ExistsWithID(ctx, m.ExtractedProjectID); !ok {
		if err != nil {
			return err
		}
		return merror.Newf(merror.NotFound, "project not found")
	}

	// validate if the topic already exists
	ok, err := c.Exists(ctx, m.Key())
	if err != nil {
		return err
	}
	if ok {
		return merror.New(merror.AlreadyExists, "Topic already exists")
	}

	// create broker topic
	err = c.createBrokerTopic(ctx, m)
	if err != nil {
		logger.Ctx(ctx).Errorw("error in creating topic in broker", "msg", err.Error())
		return err
	}

	// create regsitry entry
	return c.repo.Create(ctx, m)
}

// CreateRetryTopic creates a retry topic for the given primary topic and name
func (c *Core) CreateRetryTopic(ctx context.Context, model *Model) error {
	// create broker topic
	return c.createBrokerTopic(ctx, model)
}

// CreateDeadLetterTopic creates a deadletter topic for the given primary topic and name
func (c *Core) CreateDeadLetterTopic(ctx context.Context, model *Model) error {
	// create broker topic
	err := c.createBrokerTopic(ctx, model)
	if err != nil {
		return err
	}

	// create registry entry
	return c.repo.Create(ctx, model)
}

// Exists checks if the topic exists with a given key
func (c *Core) Exists(ctx context.Context, key string) (bool, error) {
	topicOperationCount.WithLabelValues(env, "Exists").Inc()

	startTime := time.Now()
	defer topicOperationTimeTaken.WithLabelValues(env, "Exists").Observe(float64(time.Since(startTime).Nanoseconds() / 1e9))

	ok, err := c.repo.Exists(ctx, key)
	if err != nil {
		logger.Ctx(ctx).Errorw("error in executing exists", "msg", err.Error(), "key", key)
		return false, err
	}
	return ok, nil
}

// ExistsWithName checks if the topic exists with a given name
func (c *Core) ExistsWithName(ctx context.Context, name string) (bool, error) {
	topicOperationCount.WithLabelValues(env, "ExistsWithName").Inc()

	startTime := time.Now()
	defer topicOperationTimeTaken.WithLabelValues(env, "ExistsWithName").Observe(float64(time.Since(startTime).Nanoseconds() / 1e9))

	projectID, topicName, err := ExtractTopicMetaAndValidate(ctx, name)
	if err != nil {
		return false, err
	}
	return c.Exists(ctx, common.GetBasePrefix()+Prefix+projectID+"/"+topicName)
}

// DeleteTopic deletes a topic and all resources associated with it
func (c *Core) DeleteTopic(ctx context.Context, m *Model) error {
	topicOperationCount.WithLabelValues(env, "DeleteTopic").Inc()

	startTime := time.Now()
	defer topicOperationTimeTaken.WithLabelValues(env, "DeleteTopic").Observe(float64(time.Since(startTime).Nanoseconds() / 1e9))

	if ok, err := c.projectCore.ExistsWithID(ctx, m.ExtractedProjectID); !ok {
		if err != nil {
			return err
		}
		return merror.Newf(merror.NotFound, "Project not found")
	}
	if ok, err := c.Exists(ctx, m.Key()); !ok {
		if err != nil {
			return err
		}
		return merror.New(merror.NotFound, "Topic not found")
	}
	return c.repo.Delete(ctx, m)
}

// DeleteProjectTopics deletes all topics for a given projectID
func (c *Core) DeleteProjectTopics(ctx context.Context, projectID string) error {
	topicOperationCount.WithLabelValues(env, "DeleteProjectTopics").Inc()

	startTime := time.Now()
	defer topicOperationTimeTaken.WithLabelValues(env, "DeleteProjectTopics").Observe(float64(time.Since(startTime).Nanoseconds() / 1e9))

	if projectID == "" {
		return merror.Newf(merror.InvalidArgument, "invalid projectID: %s", projectID)
	}

	prefix := common.GetBasePrefix() + Prefix + projectID

	return c.repo.DeleteTree(ctx, prefix)
}

// Get returns topic with the given key
func (c *Core) Get(ctx context.Context, key string) (*Model, error) {
	topicOperationCount.WithLabelValues(env, "Get").Inc()

	startTime := time.Now()
	defer topicOperationTimeTaken.WithLabelValues(env, "Get").Observe(float64(time.Since(startTime).Nanoseconds() / 1e9))

	projectID, topicName, err := ExtractTopicMetaAndValidate(ctx, key)
	if err != nil {
		return nil, err
	}
	prefix := common.GetBasePrefix() + Prefix + projectID + "/" + topicName

	model := &Model{}
	err = c.repo.Get(ctx, prefix, model)
	if err != nil {
		return nil, err
	}
	return model, nil
}

// createBrokerTopic creates the topic with the message broker
func (c *Core) createBrokerTopic(ctx context.Context, model *Model) error {
	admin, aerr := c.brokerStore.GetAdmin(ctx, messagebroker.AdminClientOptions{})
	if aerr != nil {
		return aerr
	}

	// Create topic requset with Broker
	_, terr := admin.CreateTopic(ctx, messagebroker.CreateTopicRequest{
		Name:          model.Name,
		NumPartitions: model.NumPartitions,
	})

	return terr
}
