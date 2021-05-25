package web

import (
	"context"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/pkg/messagebroker"

	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/internal/topic"

	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/internal/project"
	"github.com/razorpay/metro/pkg/logger"
	adminv1 "github.com/razorpay/metro/rpc/admin/v1"
	pubsubv1 "github.com/razorpay/metro/rpc/pubsub/v1"
	"google.golang.org/protobuf/types/known/emptypb"
)

type adminServer struct {
	projectCore      project.ICore
	subscriptionCore subscription.ICore
	topicCore        topic.ICore
	brokerStore      brokerstore.IBrokerStore
}

func newAdminServer(projectCore project.ICore, subscriptionCore subscription.ICore, topicCore topic.ICore, brokerStore brokerstore.IBrokerStore) *adminServer {
	return &adminServer{projectCore, subscriptionCore, topicCore, brokerStore}
}

// CreateProject creates a new project
func (s adminServer) CreateProject(ctx context.Context, req *pubsubv1.Project) (*pubsubv1.Project, error) {
	logger.Ctx(ctx).Infow("request received to create project", "id", req.ProjectId)
	p, err := project.GetValidatedModelForCreate(ctx, req)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}
	err = s.projectCore.CreateProject(ctx, p)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}
	return req, nil
}

// DeleteProject creates a new project
func (s adminServer) DeleteProject(ctx context.Context, req *pubsubv1.Project) (*emptypb.Empty, error) {
	logger.Ctx(ctx).Infow("request received to delete project", "id", req.ProjectId)
	p, err := project.GetValidatedModelForDelete(ctx, req)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	// Delete all subscriptions of the project first
	err = s.subscriptionCore.DeleteProjectSubscriptions(ctx, p.ProjectID)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	// Delete all topics of the project
	err = s.topicCore.DeleteProjectTopics(ctx, p.ProjectID)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	// Delete the project
	err = s.projectCore.DeleteProject(ctx, p)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	return &emptypb.Empty{}, nil
}

func (s adminServer) CreateTopic(ctx context.Context, req *adminv1.AdminTopic) (*adminv1.AdminTopic, error) {

	logger.Ctx(ctx).Infow("received admin request to create topic",
		"name", req.Name, "num_partitions", req.NumPartitions)

	m, err := topic.GetValidatedAdminModel(ctx, req)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	err = s.topicCore.CreateTopic(ctx, m)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	admin, aerr := s.brokerStore.GetAdmin(ctx, messagebroker.AdminClientOptions{})
	if aerr != nil {
		return nil, merror.ToGRPCError(aerr)
	}

	// create primary topic
	_, terr := admin.CreateTopic(ctx, messagebroker.CreateTopicRequest{
		Name:          req.GetName(),
		NumPartitions: m.NumPartitions,
	})
	if terr != nil {
		return nil, merror.ToGRPCError(terr)
	}

	return req, nil
}
