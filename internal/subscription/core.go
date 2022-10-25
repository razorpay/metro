package subscription

import (
	"context"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/internal/project"
	"github.com/razorpay/metro/internal/topic"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

// ICore is an interface over subscription core
type ICore interface {
	CreateSubscription(ctx context.Context, subscription *Model) error
	UpdateSubscription(ctx context.Context, uModel *Model) error
	Exists(ctx context.Context, key string) (bool, error)
	DeleteSubscription(ctx context.Context, m *Model) error
	DeleteProjectSubscriptions(ctx context.Context, projectID string) error
	GetTopicFromSubscriptionName(ctx context.Context, subscription string) (string, error)
	ListKeys(ctx context.Context, prefix string) ([]string, error)
	List(ctx context.Context, prefix string) ([]*Model, error)
	Get(ctx context.Context, key string) (*Model, error)
	Migrate(ctx context.Context, names []string) error
	RescaleSubTopics(ctx context.Context, topicModel *topic.Model) error
}

// Core implements all business logic for a subscription
type Core struct {
	repo        IRepo
	projectCore project.ICore
	topicCore   topic.ICore
	brokerStore brokerstore.IBrokerStore
}

// NewCore returns an instance of Core
func NewCore(repo IRepo, projectCore project.ICore, topicCore topic.ICore, brokerStore brokerstore.IBrokerStore) ICore {
	return &Core{repo, projectCore, topicCore, brokerStore}
}

// CreateSubscription creates a subscription for a given topic
func (c *Core) CreateSubscription(ctx context.Context, m *Model) error {
	subscriptionOperationCount.WithLabelValues(env, "CreateSubscription").Inc()

	startTime := time.Now()
	defer func() {
		subscriptionOperationTimeTaken.WithLabelValues(env, "CreateSubscription").Observe(time.Now().Sub(startTime).Seconds())
	}()

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

	var topicModel *topic.Model
	if topicModel, err = c.topicCore.Get(ctx, m.GetTopic()); err != nil {
		return err
	}

	err = c.topicCore.CreateSubscriptionTopic(ctx, &topic.Model{
		Name:               m.GetSubscriptionTopic(),
		ExtractedTopicName: m.ExtractedSubscriptionName,
		ExtractedProjectID: m.ExtractedTopicProjectID,
		NumPartitions:      topicModel.NumPartitions,
	})

	if err != nil {
		logger.Ctx(ctx).Errorw("failed to create subscription topic for subscription", "name", m.GetSubscriptionTopic(), "error", err.Error())
		return err
	}

	err = c.topicCore.CreateRetryTopic(ctx, &topic.Model{
		Name:               m.GetRetryTopic(),
		ExtractedTopicName: m.ExtractedSubscriptionName + topic.RetryTopicSuffix,
		ExtractedProjectID: m.ExtractedTopicProjectID,
		NumPartitions:      topicModel.NumPartitions,
	})

	if err != nil {
		logger.Ctx(ctx).Errorw("failed to create retry topic for subscription", "name", m.GetRetryTopic(), "error", err.Error())
		return err
	}

	// this creates the needed delay topics in the broker
	err = createDelayTopics(ctx, m, c.topicCore, topicModel)
	if err != nil {
		logger.Ctx(ctx).Errorw("failed to create delay topics", "error", err.Error())
		return err
	}

	// for subscription over deadletter topics, skip the deadletter topic creation
	if topicModel.IsDeadLetterTopic() {
		m.Labels["isDLQSubscription"] = "true"
	} else {
		m.Labels["isDLQSubscription"] = "false"
		// create dead-letter topic for subscription
		err = c.topicCore.CreateDeadLetterTopic(ctx, &topic.Model{
			Name:               m.GetDeadLetterTopic(),
			ExtractedTopicName: m.ExtractedSubscriptionName + topic.DeadLetterTopicSuffix,
			ExtractedProjectID: m.ExtractedTopicProjectID,
			NumPartitions:      topicModel.NumPartitions,
		})

		if err != nil {
			logger.Ctx(ctx).Errorw("failed to create dead letter topic for subscription", "name", m.GetDeadLetterTopic(), "error", err.Error())
			return err
		}
	}

	err = c.repo.Save(ctx, m)

	if err == nil {
		subscriptionCount.WithLabelValues(
			env,
			m.ExtractedTopicName,
			m.GetSubscriptionType(),
			strconv.FormatBool(m.IsFilteringEnabled()),
			strconv.FormatBool(m.EnableMessageOrdering)).Inc()
	}
	return err
}

// UpdateSubscription - Updates a given subscription
func (c *Core) UpdateSubscription(ctx context.Context, uModel *Model) error {
	subscriptionOperationCount.WithLabelValues(env, "UpdateSubscription").Inc()

	startTime := time.Now()
	defer func() {
		subscriptionOperationTimeTaken.WithLabelValues(env, "UpdateSubscription").Observe(time.Now().Sub(startTime).Seconds())
	}()

	if ok, err := c.projectCore.ExistsWithID(ctx, uModel.ExtractedSubscriptionProjectID); !ok {
		if err != nil {
			return err
		}
		logger.Ctx(ctx).Errorw("subscription project not found", "name", uModel.ExtractedSubscriptionProjectID)
		return merror.New(merror.NotFound, "project not found")
	}

	if ok, err := c.Exists(ctx, uModel.Key()); !ok {
		if err != nil {
			return err
		}
		logger.Ctx(ctx).Errorw("subscription not found", "name", uModel.Name)
		return merror.New(merror.NotFound, "subscription not found")
	}

	// This will create all delay topics based on updated subscription config.
	topicModel, err := c.topicCore.Get(ctx, uModel.GetTopic())
	if err != nil {
		logger.Ctx(ctx).Errorw("failed to get topic model", "error", err.Error())
		return err
	}

	err = createDelayTopics(ctx, uModel, c.topicCore, topicModel)
	if err != nil {
		logger.Ctx(ctx).Errorw("failed to create delay topics", "error", err.Error())
		return err
	}

	return c.repo.Save(ctx, uModel)
}

// Exists checks if subscription exists for a given key
func (c *Core) Exists(ctx context.Context, key string) (bool, error) {
	subscriptionOperationCount.WithLabelValues(env, "Exists").Inc()

	startTime := time.Now()
	defer func() {
		subscriptionOperationTimeTaken.WithLabelValues(env, "Exists").Observe(time.Now().Sub(startTime).Seconds())
	}()

	ok, err := c.repo.Exists(ctx, key)
	if err != nil {
		logger.Ctx(ctx).Errorw("error in executing exists", "msg", err.Error(), "key", key)
		return false, err
	}
	return ok, nil
}

// DeleteSubscription deletes a subscription
func (c *Core) DeleteSubscription(ctx context.Context, m *Model) error {
	subscriptionOperationCount.WithLabelValues(env, "DeleteSubscription").Inc()

	startTime := time.Now()
	defer func() {
		subscriptionOperationTimeTaken.WithLabelValues(env, "DeleteSubscription").Observe(time.Since(startTime).Seconds())
	}()

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
	// cleaning up all subscription topics from broker and consul
	if err := c.deleteSubscriptionTopics(ctx, m); err != nil {
		return err
	}
	return c.repo.Delete(ctx, m)
}

// DeleteProjectSubscriptions deletes all subscriptions for the given projectID
func (c *Core) DeleteProjectSubscriptions(ctx context.Context, projectID string) error {
	subscriptionOperationCount.WithLabelValues(env, "DeleteProjectSubscriptions").Inc()

	startTime := time.Now()
	defer func() {
		subscriptionOperationTimeTaken.WithLabelValues(env, "DeleteProjectSubscriptions").Observe(time.Now().Sub(startTime).Seconds())
	}()

	if projectID == "" {
		return merror.Newf(merror.InvalidArgument, "invalid projectID: %s", projectID)
	}

	prefix := common.GetBasePrefix() + Prefix + projectID

	return c.repo.DeleteTree(ctx, prefix)
}

// GetTopicFromSubscriptionName returns topic from subscription
func (c *Core) GetTopicFromSubscriptionName(ctx context.Context, subscription string) (string, error) {
	subscriptionOperationCount.WithLabelValues(env, "GetTopicFromSubscriptionName").Inc()

	startTime := time.Now()
	defer func() {
		subscriptionOperationTimeTaken.WithLabelValues(env, "GetTopicFromSubscriptionName").Observe(time.Now().Sub(startTime).Seconds())
	}()

	m, err := c.Get(ctx, subscription)

	if err != nil {
		return "", err
	}
	return m.GetTopic(), nil
}

// ListKeys gets all subscription keys
func (c *Core) ListKeys(ctx context.Context, prefix string) ([]string, error) {
	subscriptionOperationCount.WithLabelValues(env, "ListKeys").Inc()

	startTime := time.Now()
	defer func() {
		subscriptionOperationTimeTaken.WithLabelValues(env, "ListKeys").Observe(time.Now().Sub(startTime).Seconds())
	}()

	prefix = common.GetBasePrefix() + prefix
	return c.repo.ListKeys(ctx, prefix)
}

// List gets slice of subscriptions starting with given prefix
func (c *Core) List(ctx context.Context, prefix string) ([]*Model, error) {
	subscriptionOperationCount.WithLabelValues(env, "List").Inc()

	startTime := time.Now()
	defer func() {
		subscriptionOperationTimeTaken.WithLabelValues(env, "List").Observe(time.Now().Sub(startTime).Seconds())
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

// Get returns subscription with the given key
func (c *Core) Get(ctx context.Context, key string) (*Model, error) {
	subscriptionOperationCount.WithLabelValues(env, "Get").Inc()

	startTime := time.Now()
	defer func() {
		subscriptionOperationTimeTaken.WithLabelValues(env, "Get").Observe(time.Now().Sub(startTime).Seconds())
	}()

	projectID, subscriptionName, err := extractSubscriptionMetaAndValidate(ctx, key)
	if err != nil {
		return nil, err
	}
	prefix := common.GetBasePrefix() + Prefix + projectID + "/" + subscriptionName

	logger.Ctx(ctx).Infow("fetching subscription", "key", prefix)

	model := &Model{}
	err = c.repo.Get(ctx, prefix, model)
	if err != nil {
		return nil, err
	}
	return model, nil
}

// createDelayTopics - creates needed delay topics for a subscription
func createDelayTopics(ctx context.Context, m *Model, topicCore topic.ICore, topicModel *topic.Model) error {
	if m == nil || topicCore == nil {
		return nil
	}

	delayTopics := m.GetDelayTopicsByBackoff()
	for _, delayTopic := range delayTopics {

		tModel, terr := topic.GetValidatedModel(ctx, &metrov1.Topic{
			Name:            delayTopic,
			NumOfPartitions: int32(topicModel.NumPartitions),
		})
		if terr != nil {
			logger.Ctx(ctx).Errorw("failed to create validated topic model", "delayTopic", delayTopic, "error", terr.Error())
			return terr
		}

		logger.Ctx(ctx).Infow("topic model ", ":", tModel)
		err := topicCore.CreateTopic(ctx, tModel)
		if val, ok := err.(*merror.MError); ok {
			// in-case users delete and re-create a subscription
			// we should ideally be deleting all associated topics
			// temp check, remove once delete subscription feature is live
			if val.Code() == merror.AlreadyExists {
				continue
			}
		} else if err != nil {
			logger.Ctx(ctx).Errorw("failed to create delay topic for subscription", "name", delayTopic, "error", err.Error())
			return err
		}
	}

	return nil
}

// RescaleSubTopics - Get all the subs and rescale all the Retry/Delay/DLQ topics
func (c *Core) RescaleSubTopics(ctx context.Context, topicModel *topic.Model) error {
	projectList, err := c.projectCore.ListKeys(ctx)
	if err != nil {
		return err
	}
	var completeSubList []*Model

	for _, projectKey := range projectList {
		subList, subErr := c.List(ctx, Prefix+project.FetchProjectID(ctx, projectKey))
		if subErr != nil {
			return subErr
		}
		completeSubList = append(completeSubList, subList...)
	}
	admin, err := c.brokerStore.GetAdmin(ctx, messagebroker.AdminClientOptions{})
	if err != nil {
		return merror.ToGRPCError(err)
	}

	for _, m := range completeSubList {
		if m.Topic != topicModel.Name {
			continue
		}
		// modify topic partitions
		retryTopicName := m.GetRetryTopic()
		_, err := admin.AddTopicPartitions(ctx, messagebroker.AddTopicPartitionRequest{
			Name:          retryTopicName,
			NumPartitions: topicModel.NumPartitions,
		})
		if err != nil {
			return merror.ToGRPCError(err)
		}
		retryModel := &topic.Model{
			Name:               retryTopicName,
			ExtractedTopicName: m.ExtractedSubscriptionName + topic.RetryTopicSuffix,
			ExtractedProjectID: m.ExtractedTopicProjectID,
			NumPartitions:      topicModel.NumPartitions,
		}
		retryTopicError := c.topicCore.UpdateTopic(ctx, retryModel)
		if retryTopicError != nil {
			logger.Ctx(ctx).Error(
				"Error in executing Retry Topic Rescaling: ",
				retryTopicError.Error(),
				" TopicName: ",
				retryModel.Name)
		}

		delayTopics := m.GetDelayTopics()
		for _, delayTopic := range delayTopics {
			_, err := admin.AddTopicPartitions(ctx, messagebroker.AddTopicPartitionRequest{
				Name:          delayTopic,
				NumPartitions: topicModel.NumPartitions,
			})

			if val, ok := err.(*merror.MError); ok {
				if val.Code() == merror.NotFound {
					continue
				}
			} else if err != nil {
				return merror.ToGRPCError(err)
			}
			delayModel := &topic.Model{
				Name:               delayTopic,
				ExtractedTopicName: topic.GetTopicNameOnly(delayTopic),
				ExtractedProjectID: m.ExtractedTopicProjectID,
				NumPartitions:      topicModel.NumPartitions,
			}
			delayTopicError := c.topicCore.UpdateTopic(ctx, delayModel)
			if delayTopicError != nil {
				logger.Ctx(ctx).Error(
					"Error in executing Delay Topic Rescaling: ",
					delayTopicError.Error(),
					" TopicName: ",
					delayModel.Name)
			}
		}

		if !topicModel.IsDeadLetterTopic() {
			dlqTopicName := m.GetDeadLetterTopic()
			_, err := admin.AddTopicPartitions(ctx, messagebroker.AddTopicPartitionRequest{
				Name:          dlqTopicName,
				NumPartitions: topicModel.NumPartitions,
			})
			if err != nil {
				return merror.ToGRPCError(err)
			}
			dlqModel := &topic.Model{
				Name:               dlqTopicName,
				ExtractedTopicName: m.ExtractedSubscriptionName + topic.DeadLetterTopicSuffix,
				ExtractedProjectID: m.ExtractedTopicProjectID,
				NumPartitions:      topicModel.NumPartitions,
			}
			dlqTopicError := c.topicCore.UpdateTopic(ctx, dlqModel)
			if dlqTopicError != nil {
				logger.Ctx(ctx).Error(
					"Error in executing DLQ Topic Rescaling: ",
					dlqTopicError.Error(),
					" TopicName: ",
					dlqModel.Name)
			}
		}
	}
	return nil
}

// Migrate takes care of backfilling subscription topics for existing subscriptions.
// This is an idempotent operation that creates topics for each subscription.
// Migrate can be modified in the future for other use-cases as well.
func (c *Core) Migrate(ctx context.Context, names []string) error {

	subscriptionsToUpdate := make([]*Model, 0)
	if len(names) == 0 {
		models, err := c.List(ctx, Prefix)
		if err != nil {
			return err
		}
		subscriptionsToUpdate = append(subscriptionsToUpdate, models...)
	} else {
		for _, name := range names {
			model, err := c.Get(ctx, name)
			if err != nil {
				return err
			}
			subscriptionsToUpdate = append(subscriptionsToUpdate, model)
		}
	}

	logger.Ctx(ctx).Infow("migration: found subscriptions to migrate", "count", len(subscriptionsToUpdate))

	updatedSubCount := 0
	for _, sub := range subscriptionsToUpdate {
		topicModel, err := c.topicCore.Get(ctx, sub.GetTopic())
		if err != nil {
			logger.Ctx(ctx).Errorw("migration: failed to fetch topic for subscription", "subscription", sub.Name, "topic", sub.GetTopic())
			continue
		}

		// Currently we do not have a way to check if a kafka topic exists except for a topic metadata call.
		// But topic create returns an error if topic already exists.
		err = c.topicCore.CreateSubscriptionTopic(ctx, &topic.Model{
			Name:               sub.GetSubscriptionTopic(),
			ExtractedTopicName: sub.ExtractedSubscriptionName,
			ExtractedProjectID: sub.ExtractedTopicProjectID,
			NumPartitions:      topicModel.NumPartitions,
		})
		if err != nil {
			updatedSubCount++
			logger.Ctx(ctx).Infow("migration: updating subscription", "subscription", sub.Name)
		}
	}

	logger.Ctx(ctx).Infow("migration: request completed.", "subscriptionsUpdated", updatedSubCount)
	return nil
}

func (c *Core) deleteSubscriptionTopics(ctx context.Context, m *Model) error {
	sub, err := c.Get(ctx, m.Name)
	if err != nil {
		return err
	}
	if sub.GetTopic() == "" {
		return errors.New("Topic doesn't exist for subscription")
	}

	m.Topic = sub.Topic
	m.ExtractedTopicProjectID = sub.ExtractedTopicProjectID
	m.ExtractedTopicName = sub.ExtractedTopicName
	subsTopics := getSubscriptionTopics(m)
	for _, subsTopic := range subsTopics {
		err := c.topicCore.DeleteTopic(ctx, &topic.Model{
			Name:               subsTopic,
			ExtractedProjectID: m.ExtractedSubscriptionProjectID,
			ExtractedTopicName: topic.GetTopicNameOnly(subsTopic),
		})
		// For subscription topics that do not exist, ignore the errors
		if val, ok := err.(*merror.MError); ok {
			if val.Code() == merror.NotFound {
				continue
			}
		} else if err != nil {
			return err
		}
	}
	return nil
}

// getSubscriptionTopics gets list of all the subscription topics
func getSubscriptionTopics(m *Model) []string {
	subsTopics := []string{
		m.GetSubscriptionTopic(),
		m.GetRetryTopic(),
	}
	subsTopics = append(subsTopics, m.GetDelayTopics()...)
	if topic.IsDLQTopic(m.ExtractedTopicName) {
		return subsTopics
	}
	return append(subsTopics, m.GetDeadLetterTopic())
}
