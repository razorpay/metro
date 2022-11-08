package web

import (
	"context"
	"errors"
	"strings"

	"github.com/opentracing/opentracing-go"
	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/credentials"
	"github.com/razorpay/metro/internal/interceptors"
	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/internal/nodebinding"
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
	nodeBindingCore  nodebinding.ICore
	brokerStore      brokerstore.IBrokerStore
}

func newAdminServer(admin *credentials.Model, projectCore project.ICore, subscriptionCore subscription.ICore, topicCore topic.ICore, credentialCore credentials.ICore, nodebindingCore nodebinding.ICore, brokerStore brokerstore.IBrokerStore) *adminServer {
	return &adminServer{admin, projectCore, subscriptionCore, topicCore, credentialCore, nodebindingCore, brokerStore}
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

	// Fetch existing topic before updating
	existingTopic, err := s.topicCore.Get(ctx, m.Name)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	if int(req.NumPartitions) != existingTopic.NumPartitions {
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

		err := s.subscriptionCore.RescaleSubTopics(ctx, m)
		if err != nil {
			return &emptypb.Empty{}, err
		}
	}

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

// get the ProjectCredentials for the given projectId and username
func (s adminServer) GetProjectCredentials(ctx context.Context, req *metrov1.ProjectCredentials) (*metrov1.ProjectCredentials, error) {
	logger.Ctx(ctx).Infow("received request to get project credentials", "projectID", req.ProjectId, "username", req.Username)

	credential, err := s.credentialCore.Get(ctx, req.ProjectId, req.Username)

	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	logger.Ctx(ctx).Infow("request to get credentials completed", "projectID", req.ProjectId, "username", req.Username)

	return &metrov1.ProjectCredentials{
		ProjectId: credential.ProjectID,
		Username:  credential.GetUsername(),
		Password:  credential.GetPassword(),
	}, nil
}

// returns list of all the project credentials for the given projectId
func (s adminServer) ListProjectCredentials(ctx context.Context, req *metrov1.ProjectCredentials) (*metrov1.ProjectCredentialsList, error) {
	logger.Ctx(ctx).Infow("received request to list project credentials", "projectID", req.ProjectId)

	projectID := req.ProjectId

	models, err := s.credentialCore.List(ctx, projectID)

	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	logger.Ctx(ctx).Infow("request to list credentials completed", "projectID", req.ProjectId)

	var credentials []*metrov1.ProjectCredentials
	for _, m := range models {
		hiddenPwd, err := m.GetHiddenPassword()
		if err != nil {
			logger.Ctx(ctx).Errorw("error occurred in masking credentials", "errMsg", err.Error(), "projectID", m.ProjectID, "username", m.Username)
			continue
		}
		credentials = append(credentials, &metrov1.ProjectCredentials{
			ProjectId: m.ProjectID,
			Username:  m.GetUsername(),
			Password:  hiddenPwd,
		})
	}
	return &metrov1.ProjectCredentialsList{
		ProjectCredentials: credentials,
	}, nil
}

func (s adminServer) MigrateSubscriptions(ctx context.Context, subscriptions *metrov1.Subscriptions) (*emptypb.Empty, error) {
	logger.Ctx(ctx).Infow("received request to migrate subscriptions", "subscriptions", subscriptions.GetNames())

	err := s.nodeBindingCore.TriggerNodeBindingRefresh(ctx)
	if err != nil {
		return &emptypb.Empty{}, err
	}
	// err := s.subscriptionCore.Migrate(ctx, subscriptions.GetNames())
	// if err != nil {
	// 	return nil, merror.ToGRPCError(err)
	// }

	// logger.Ctx(ctx).Infow("request to migrate subscriptions completed", "subscriptions", subscriptions.GetNames())
	return &emptypb.Empty{}, nil
}

// normalizeTopicName returns the actual topic name used in message broker
func normalizeTopicName(name string) string {
	return strings.ReplaceAll(name, "/", "_")
}

func (s adminServer) CleanupTopics(ctx context.Context, projects *metrov1.Projects) (*metrov1.Topics, error) {
	logger.Ctx(ctx).Infow("received request to cleanup topics", "projects", projects.GetProjects())
	tops := metrov1.Topics{}
	validProjects, err := s.projectCore.ListKeys(ctx)
	if err != nil {
		logger.Ctx(ctx).Errorw("Failed to fetch projects list", "error", err.Error())
		return &metrov1.Topics{}, err
	}

	validTopics := make(map[string]bool, 0)
	for _, validProject := range validProjects {
		projectName := strings.Split(validProject, "/")
		if len(projectName) != 3 {
			return &metrov1.Topics{}, errors.New("incompatible project name")
		}
		topics, err := s.topicCore.List(ctx, topic.Prefix+projectName[2])
		if err != nil {
			logger.Ctx(ctx).Errorw("failed to fetch project topics", "project", projectName[2])
			return &metrov1.Topics{}, err
		}
		for _, t := range topics {
			validTopics[normalizeTopicName(t.Name)] = true
		}
		subs, err := s.subscriptionCore.List(ctx, subscription.Prefix+projectName[2])
		if err != nil {
			logger.Ctx(ctx).Errorw("failed to fetch project subscriptions", "project", projectName[2])
			return &metrov1.Topics{}, err
		}
		for _, sub := range subs {

			for _, v := range sub.GetDelayTopics() {
				validTopics[normalizeTopicName(v)] = true
			}
			validTopics[normalizeTopicName(sub.GetDeadLetterTopic())] = true
			validTopics[normalizeTopicName(sub.GetRetryTopic())] = true
			// validTopics[sub.GetSubscriptionTopic()] = true ###Internal topics are not used and hence up for deletion
		}
	}

	admin, aerr := s.brokerStore.GetAdmin(ctx, messagebroker.AdminClientOptions{})
	if aerr != nil {
		return nil, merror.ToGRPCError(aerr)
	}

	for _, p := range projects.Projects {

		allTopics, err := admin.FetchProjectTopics(ctx, project.Prefix+p)
		if err != nil {
			logger.Ctx(ctx).Errorw("Failed to fetch topics for project from messagebroker", "project", p)
			return &metrov1.Topics{}, err
		}
		logger.Ctx(ctx).Infow("valids", "valid", validTopics, "allTopics", allTopics)
		for t := range allTopics {
			t = normalizeTopicName(t)
			if _, ok := validTopics[t]; !ok {
				tops.Names = append(tops.Names, t)
				if projects.HardDelete {
					dtresp, err := admin.DeleteTopic(ctx, messagebroker.DeleteTopicRequest{Name: t})
					if err != nil {
						logger.Ctx(ctx).Errorw("Failed to delete topics", "error", err.Error())
					}
					logger.Ctx(ctx).Infow("Successfully deleted topic", "resp", dtresp)
				}
			}

		}
	}

	return &tops, nil
}

func (s adminServer) SetupRetentionPolicy(ctx context.Context, topics *metrov1.Topics) (*metrov1.Topics, error) {
	logger.Ctx(ctx).Infow("received request to set up retention policy")
	updatedTopics, err := s.topicCore.SetupTopicRetentionConfigs(ctx, topics.GetNames())
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}
	return &metrov1.Topics{Names: updatedTopics}, nil
}

func (s adminServer) AuthFuncOverride(ctx context.Context, _ string, _ interface{}) (context.Context, error) {
	return interceptors.AdminAuth(ctx, s.admin)
}
