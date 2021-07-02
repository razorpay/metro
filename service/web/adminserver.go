package web

import (
	"context"

	"github.com/opentracing/opentracing-go"
	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/credentials"
	"github.com/razorpay/metro/internal/interceptors"
	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/internal/project"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/internal/topic"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"google.golang.org/protobuf/types/known/emptypb"
)

type adminServer struct {
	admin            *credentials.Model
	projectCore      project.ICore
	subscriptionCore subscription.ICore
	topicCore        topic.ICore
	credentialCore   credentials.ICore
	brokerStore      brokerstore.IBrokerStore
}

func newAdminServer(admin *credentials.Model, projectCore project.ICore, subscriptionCore subscription.ICore, topicCore topic.ICore, credentialCore credentials.ICore, brokerStore brokerstore.IBrokerStore) *adminServer {
	return &adminServer{admin, projectCore, subscriptionCore, topicCore, credentialCore, brokerStore}
}

// CreateProject creates a new project
func (s adminServer) CreateProject(ctx context.Context, req *metrov1.Project) (*metrov1.Project, error) {
	logger.Ctx(ctx).Infow("request received to create project", "id", req.ProjectId)
	span, ctx := opentracing.StartSpanFromContext(ctx, "AdminServer.CreateProject")
	defer span.Finish()

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
	span, ctx := opentracing.StartSpanFromContext(ctx, "AdminServer.DeleteProject")
	defer span.Finish()

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

	logger.Ctx(ctx).Infow("admin request to modify topic completed",
		"name", req.Name, "new_partitions", req.NumPartitions)

	return &emptypb.Empty{}, nil
}

func (s adminServer) CreateProjectCredentials(ctx context.Context, req *metrov1.ProjectCredentials) (*metrov1.ProjectCredentials, error) {

	logger.Ctx(ctx).Infow("received request to create new credentials", "projectID", req.ProjectId)

	credential, err := credentials.GetValidatedModelForCreate(ctx, req)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	err = s.credentialCore.Create(ctx, credential)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	logger.Ctx(ctx).Infow("request to create new credentials completed", "projectID", req.ProjectId, "username", credential.GetUsername())

	return &metrov1.ProjectCredentials{
		ProjectId: credential.ProjectID,
		Username:  credential.GetUsername(),
		Password:  credential.GetPassword(),
	}, nil
}

func (s adminServer) DeleteProjectCredentials(ctx context.Context, req *metrov1.ProjectCredentials) (*emptypb.Empty, error) {
	logger.Ctx(ctx).Infow("received request to delete existing credentials", "projectID", req.ProjectId, "username", req.Username)

	// validate the username provided
	if !credentials.IsValidUsername(req.Username) {
		return nil, merror.New(merror.InvalidArgument, "invalid username")
	}

	projectID := credentials.GetProjectIDFromUsername(req.Username)

	// query existing credential
	credential, err := s.credentialCore.Get(ctx, projectID, req.Username)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	// delete existing credential
	err = s.credentialCore.Delete(ctx, credential)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	logger.Ctx(ctx).Infow("request to delete existing credentials completed", "projectID", req.ProjectId, "username", req.Username)

	return &emptypb.Empty{}, nil
}

func (s adminServer) AuthFuncOverride(ctx context.Context, _ string) (context.Context, error) {
	return interceptors.AdminAuth(ctx, s.admin)
}
