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
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
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
func (s adminServer) CreateProject(ctx context.Context, req *metrov1.Project) (*metrov1.Project, error) {
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
func (s adminServer) DeleteProject(ctx context.Context, req *metrov1.Project) (*emptypb.Empty, error) {
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

// ModifyTopic modify an existing topic
func (s adminServer) ModifyTopic(ctx context.Context, req *metrov1.AdminTopic) (*emptypb.Empty, error) {

	logger.Ctx(ctx).Infow("received admin request to modify topic",
		"name", req.Name, "num_partitions", req.NumPartitions)

	m, err := topic.GetValidatedTopicForAdminUpdate(ctx, req)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	// check for valid topic name
	exists, eerr := s.topicCore.Exists(ctx, m.Key())
	if eerr != nil {
		return nil, merror.ToGRPCError(err)
	}
	if !exists {
		return nil, merror.New(merror.NotFound, "topic not found")
	}

	admin, aerr := s.brokerStore.GetAdmin(ctx, messagebroker.AdminClientOptions{})
	if aerr != nil {
		return nil, merror.ToGRPCError(aerr)
	}

	// modify topic partitions
	_, terr := admin.AddTopicPartitions(ctx, messagebroker.AddTopicPartitionRequest{
		Name:          req.GetName(),
		NumPartitions: m.NumPartitions,
	})
	if terr != nil {
		return nil, merror.ToGRPCError(terr)
	}

	// finally update topic with the updated partition count
	m.NumPartitions = int(req.NumPartitions)
	if uerr := s.topicCore.UpdateTopic(ctx, m); uerr != nil {
		return nil, merror.ToGRPCError(uerr)
	}

	return &emptypb.Empty{}, nil
}
