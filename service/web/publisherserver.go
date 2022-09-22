package web

import (
	"context"

	"github.com/google/uuid"
	"github.com/opentracing/opentracing-go"
	"github.com/razorpay/metro/internal/app"
	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/credentials"
	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/internal/nodebinding"
	"github.com/razorpay/metro/internal/project"
	"github.com/razorpay/metro/internal/publisher"
	"github.com/razorpay/metro/internal/tasks"
	"github.com/razorpay/metro/internal/topic"
	configreader "github.com/razorpay/metro/pkg/config"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/registry"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"google.golang.org/protobuf/types/known/emptypb"
)

type publisherServer struct {
	projectCore     project.ICore
	brokerStore     brokerstore.IBrokerStore
	topicCore       topic.ICore
	credentialsCore credentials.ICore
	publisher       publisher.IPublisher
}

func newPublisherServer(projectCore project.ICore, brokerStore brokerstore.IBrokerStore, topicCore topic.ICore, credentialsCore credentials.ICore, publisher publisher.IPublisher) *publisherServer {
	return &publisherServer{projectCore: projectCore, brokerStore: brokerStore, topicCore: topicCore, credentialsCore: credentialsCore, publisher: publisher}
}

// Produce messages to a topic
func (s publisherServer) Publish(ctx context.Context, req *metrov1.PublishRequest) (*metrov1.PublishResponse, error) {
	logger.Ctx(ctx).Infow("PublishServer: produce request received", "req", req.Topic)
	span, ctx := opentracing.StartSpanFromContext(ctx, "PublisherServer.Publish", opentracing.Tags{
		"topic": req.Topic,
	})
	defer span.Finish()
	var appConfig *registry.Config
	env := app.GetEnv()
	err := configreader.NewDefaultConfig().Load(env, &appConfig)

	r, err := registry.NewRegistry(appConfig)
	nodeBindingCore := nodebinding.NewCore(nodebinding.NewRepo(r))
	topicCore := topic.NewCore(topic.NewRepo(r), s.projectCore, s.brokerStore)
	// Init Publisher task, this run the watchers on Registry
	publisherTask, err := tasks.NewPublisherTask(
		uuid.New().String(),
		r,
		topicCore,
		nodeBindingCore,
	)
	if publisherTask.CheckIfTopicExists(ctx, req.Topic) {
		logger.Ctx(ctx).Infow("PublishServer: Topic exists inside the cache..", "req", req.Topic)
	} else if ok, err := s.topicCore.ExistsWithName(ctx, req.Topic); err != nil {
		return nil, merror.ToGRPCError(err)
	} else if !ok {
		return nil, merror.New(merror.NotFound, "topic not found").ToGRPCError()
	}

	if err := publisher.ValidatePublishRequest(ctx, req); err != nil {
		return nil, merror.ToGRPCError(err)
	}

	msgIDs, err := s.publisher.Publish(ctx, req)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}
	logger.Ctx(ctx).Infow("produce request completed", "req", req.Topic, "message_ids", msgIDs)
	return &metrov1.PublishResponse{MessageIds: msgIDs}, nil
}

// CreateTopic creates a new topic
func (s publisherServer) CreateTopic(ctx context.Context, req *metrov1.Topic) (*metrov1.Topic, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "PublisherServer.CreateTopic", opentracing.Tags{
		"topic": req.Name,
	})
	defer span.Finish()

	logger.Ctx(ctx).Infow("received request to create topic", "name", req.Name)
	m, err := topic.GetValidatedModel(ctx, req)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	err = s.topicCore.CreateTopic(ctx, m)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	return req, nil
}

// Delete a topic
func (s publisherServer) DeleteTopic(ctx context.Context, req *metrov1.DeleteTopicRequest) (*emptypb.Empty, error) {
	logger.Ctx(ctx).Infow("received request to delete topic", "name", req.Topic)

	span, ctx := opentracing.StartSpanFromContext(ctx, "PublisherServer.DeleteTopic", opentracing.Tags{
		"topic": req.Topic,
	})
	defer span.Finish()

	// Delete topic but not the subscriptions for it
	// the subscriptions would get tagged to _deleted_topic_
	m, err := topic.GetValidatedModel(ctx, &metrov1.Topic{Name: req.Topic})
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	err = s.topicCore.DeleteTopic(ctx, m)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s publisherServer) ListProjectTopics(ctx context.Context,
	req *metrov1.ListProjectTopicsRequest) (*metrov1.ListProjectTopicsResponse, error) {
	logger.Ctx(ctx).Infow("publisherServer: received request to list project topics", "project_id", req.ProjectId)

	span, ctx := opentracing.StartSpanFromContext(ctx, "PublisherServer.ListProjectTopics", opentracing.Tags{
		"project": req.ProjectId,
	})
	defer span.Finish()

	topics, err := s.topicCore.List(ctx, topic.Prefix+req.ProjectId)

	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	res := []string{}
	for _, t := range topics {
		if t.IsPrimaryTopic() {
			res = append(res, t.ExtractedTopicName)
		}
	}
	return &metrov1.ListProjectTopicsResponse{Topics: res}, nil
}

//AuthFuncOverride - Override function called by the auth interceptor
func (s publisherServer) AuthFuncOverride(ctx context.Context, fullMethodName string, req interface{}) (context.Context, error) {
	return authRequest(ctx, s.credentialsCore, fullMethodName, req)
}
