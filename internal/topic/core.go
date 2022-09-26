package topic

import (
	"context"
	"time"

	"github.com/opentracing/opentracing-go"
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
	UpdateTopic(ctx context.Context, model *Model) error
	SetupTopicRetentionConfigs(ctx context.Context, names []string) ([]string, error)
	Exists(ctx context.Context, key string) (bool, error)
	ExistsWithName(ctx context.Context, name string) (bool, error)
	DeleteTopic(ctx context.Context, m *Model) error
	DeleteSubscriptionTopic(ctx context.Context, m *Model) error
	DeleteProjectTopics(ctx context.Context, projectID string) error
	Get(ctx context.Context, key string) (*Model, error)
	CreateSubscriptionTopic(ctx context.Context, model *Model) error
	CreateRetryTopic(ctx context.Context, model *Model) error
	CreateDeadLetterTopic(ctx context.Context, model *Model) error
	List(ctx context.Context, prefix string) ([]*Model, error)
}

// Core implements all business logic for a topic
type Core struct {
	repo        IRepo
	projectCore project.ICore
	brokerStore brokerstore.IBrokerStore
}

// NewCore returns an instance of Core
func NewCore(repo IRepo, projectCore project.ICore, brokerStore brokerstore.IBrokerStore) ICore {
	return &Core{repo, projectCore, brokerStore}
}

// CreateTopic implements topic creation
func (c *Core) CreateTopic(ctx context.Context, m *Model) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "TopicCore.CreateTopic")
	defer span.Finish()

	topicOperationCount.WithLabelValues(env, "CreateTopic").Inc()

	startTime := time.Now()
	defer func() {
		topicOperationTimeTaken.WithLabelValues(env, "CreateTopic").Observe(time.Now().Sub(startTime).Seconds())
	}()

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

	// create registry entry
	return c.repo.Save(ctx, m)
}

// CreateSubscriptionTopic creates a subscription topic for the given primary topic and name
func (c *Core) CreateSubscriptionTopic(ctx context.Context, model *Model) error {
	// create topic for fanout
	return c.createBrokerTopic(ctx, model)
}

// CreateRetryTopic creates a retry topic for the given primary topic and name
func (c *Core) CreateRetryTopic(ctx context.Context, model *Model) error {
	// create broker topic
	return c.createBrokerTopic(ctx, model)
}

// CreateDeadLetterTopic creates a dead letter topic for the given primary topic and name
func (c *Core) CreateDeadLetterTopic(ctx context.Context, model *Model) error {
	// create broker topic
	err := c.createBrokerTopic(ctx, model)
	if err != nil {
		return err
	}

	// create registry entry
	return c.repo.Save(ctx, model)
}

// UpdateTopic implements topic updation
func (c *Core) UpdateTopic(ctx context.Context, m *Model) error {
	startTime := time.Now()

	topicOperationCount.WithLabelValues(env, "UpdateTopic").Inc()
	defer func() {
		topicOperationTimeTaken.WithLabelValues(env, "UpdateTopic").Observe(time.Now().Sub(startTime).Seconds())
	}()

	// validate if the topic exists
	ok, err := c.Exists(ctx, m.Key())
	if err != nil {
		return err
	}
	if !ok {
		return merror.New(merror.NotFound, "topic not found")
	}

	return c.repo.Save(ctx, m)
}

// Exists checks if the topic exists with a given key
func (c *Core) Exists(ctx context.Context, key string) (bool, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "TopicCore.Exists")
	defer span.Finish()

	topicOperationCount.WithLabelValues(env, "Exists").Inc()

	startTime := time.Now()
	defer func() {
		topicOperationTimeTaken.WithLabelValues(env, "Exists").Observe(time.Now().Sub(startTime).Seconds())
	}()

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
	defer func() {
		topicOperationTimeTaken.WithLabelValues(env, "ExistsWithName").Observe(time.Now().Sub(startTime).Seconds())
	}()

	projectID, topicName, err := ExtractTopicMetaAndValidate(ctx, name)
	if err != nil {
		return false, err
	}
	return c.Exists(ctx, common.GetBasePrefix()+Prefix+projectID+"/"+topicName)
}

// DeleteTopic deletes a topic and all resources associated with it
func (c *Core) DeleteTopic(ctx context.Context, m *Model) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "TopicCore.DeleteTopic")
	defer span.Finish()

	topicOperationCount.WithLabelValues(env, "DeleteTopic").Inc()

	startTime := time.Now()
	defer func() {
		topicOperationTimeTaken.WithLabelValues(env, "DeleteTopic").Observe(time.Now().Sub(startTime).Seconds())
	}()

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
	// cleanup topics from broker
	if err := c.DeleteBrokerTopic(ctx, m); err != nil {
		return err
	}
	return c.repo.Delete(ctx, m)
}

// DeleteSubscriptionTopic deletes a subscription's topic and all resources associated with it
func (c *Core) DeleteSubscriptionTopic(ctx context.Context, m *Model) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "TopicCore.DeleteSubscriptionTopic")
	defer span.Finish()

	topicOperationCount.WithLabelValues(env, "DeleteSubscriptionTopic").Inc()

	startTime := time.Now()
	defer func() {
		topicOperationTimeTaken.WithLabelValues(env, "DeleteSubscriptionTopic").Observe(time.Now().Sub(startTime).Seconds())
	}()

	if ok, err := c.projectCore.ExistsWithID(ctx, m.ExtractedProjectID); !ok {
		if err != nil {
			return err
		}
		return merror.Newf(merror.NotFound, "Project not found")
	}
	// clean-up topic from broker
	if err := c.DeleteBrokerTopic(ctx, m); err != nil {
		return err
	}
	// since internal and retry topic are not being stored in registry no need to clean-up
	if m.IsRetryTopic() || m.IsSubscriptionInternalTopic() {
		return nil
	}
	// clean-up topic form registry
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
	defer func() {
		topicOperationTimeTaken.WithLabelValues(env, "DeleteProjectTopics").Observe(time.Now().Sub(startTime).Seconds())
	}()

	if projectID == "" {
		return merror.Newf(merror.InvalidArgument, "invalid projectID: %s", projectID)
	}

	prefix := common.GetBasePrefix() + Prefix + projectID

	return c.repo.DeleteTree(ctx, prefix)
}

// Get returns topic with the given key
func (c *Core) Get(ctx context.Context, key string) (*Model, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "TopicCore.Get")
	defer span.Finish()

	topicOperationCount.WithLabelValues(env, "Get").Inc()

	startTime := time.Now()
	defer func() {
		topicOperationTimeTaken.WithLabelValues(env, "Get").Observe(time.Now().Sub(startTime).Seconds())
	}()

	projectID, topicName, err := ExtractTopicMetaAndValidate(ctx, key)
	if err != nil {
		return nil, err
	}
	prefix := common.GetBasePrefix() + Prefix + projectID + "/" + topicName

	// check if valid topic
	if ok, err := c.ExistsWithName(ctx, key); !ok {
		if err != nil {
			return nil, err
		}
		return nil, merror.New(merror.NotFound, "topic not found")
	}

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
		Config:        model.GetRetentionConfig(),
	})

	return terr
}

// SetupTopicRetentionConfigs sets up retention policy on top of dlq topics
func (c *Core) SetupTopicRetentionConfigs(ctx context.Context, names []string) ([]string, error) {
	topicModels := make([]*Model, 0)
	if len(names) == 0 {
		models, err := c.List(ctx, Prefix)
		if err != nil {
			return nil, err
		}
		topicModels = append(topicModels, models...)
	} else {
		for _, name := range names {
			model, err := c.Get(ctx, name)
			if err != nil {
				return nil, err
			}
			topicModels = append(topicModels, model)
		}
	}

	topicConfigs := make(map[string]messagebroker.TopicConfig)
	topicsToUpdate := make([]string, 0, len(topicModels))

	// Alter the topic configs only for dead letter topic for now
	for _, model := range topicModels {
		if model.IsDeadLetterTopic() {
			topicConfigs[model.Name] = messagebroker.TopicConfig{
				Name:   model.Name,
				Config: model.GetRetentionConfig(),
			}
			topicsToUpdate = append(topicsToUpdate, model.Name)
		}
	}

	if len(topicsToUpdate) == 0 {
		return nil, nil
	}

	admin, aerr := c.brokerStore.GetAdmin(ctx, messagebroker.AdminClientOptions{})
	if aerr != nil {
		return nil, aerr
	}

	// Describe existing configs
	existingConfigs, err := admin.DescribeTopicConfigs(ctx, topicsToUpdate)
	if err != nil {
		return nil, err
	}

	// remove topics whose retention configs have not changed
	for name, existingConfig := range existingConfigs {
		if topicConfig, ok := topicConfigs[name]; ok {
			if IsRetentionPolicyUnchanged(existingConfig, topicConfig.Config) {
				delete(topicConfigs, name)
			}
		}
	}

	if len(topicConfigs) == 0 {
		return nil, nil
	}

	// Create alter topic config request with Broker
	updatedTopics, err := admin.AlterTopicConfigs(ctx, messagebroker.NewModifyConfigRequest(topicConfigs))
	if err != nil {
		return nil, err
	}

	return updatedTopics, nil
}

// List gets slice of topics starting with given prefix
func (c *Core) List(ctx context.Context, prefix string) ([]*Model, error) {
	topicOperationCount.WithLabelValues(env, "List").Inc()

	startTime := time.Now()
	defer func() {
		topicOperationTimeTaken.WithLabelValues(env, "List").Observe(time.Now().Sub(startTime).Seconds())
	}()

	prefix = common.GetBasePrefix() + prefix

	var out []*Model
	ret, err := c.repo.List(ctx, prefix)
	if err != nil {
		return nil, err
	}

	for _, obj := range ret {
		out = append(out, obj.(*Model))
	}
	return out, nil
}

// DeleteBrokerTopic deletes the topic from the message broker
func (c *Core) DeleteBrokerTopic(ctx context.Context, model *Model) error {
	// only delete the topic if topic clean-up config is enabled
	if !c.brokerStore.IsTopicCleanUpEnabled(ctx) {
		logger.Ctx(ctx).Infow("brokerstore topic cleanup not enabled")
		return nil
	}
	admin, aerr := c.brokerStore.GetAdmin(ctx, messagebroker.AdminClientOptions{})
	if aerr != nil {
		return aerr
	}
	// deletes topic from Broker
	_, terr := admin.DeleteTopic(ctx, messagebroker.DeleteTopicRequest{Name: model.Name})
	return terr
}
