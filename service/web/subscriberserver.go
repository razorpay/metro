package web

import (
	"bytes"
	"context"
	"fmt"
	"net/http"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/imdario/mergo"
	"github.com/mennanov/fmutils"
	"github.com/opentracing/opentracing-go"
	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/credentials"
	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/internal/project"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/internal/topic"
	"github.com/razorpay/metro/pkg/httpclient"
	"github.com/razorpay/metro/pkg/logger"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/razorpay/metro/service/web/stream"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	consumePlaneAddressFormat string = "https://%s-%d.%s"
	consumePlaneDeployment    string = "metro-consume-plane"
	acknowledgeRequestFormat  string = "%s/v1/%s:acknowledge"
	fetchRequestFormat        string = "%s/v1/%s:fetch"
	modAckRequestFormat       string = "%s/v1/%s:modifyAckDeadline"
)

type subscriberserver struct {
	projectCore      project.ICore
	brokerStore      brokerstore.IBrokerStore
	subscriptionCore subscription.ICore
	topicCore        topic.ICore
	credentialCore   credentials.ICore
	replicaCount     int
	consumeNodes     map[int]string
	marshaler        jsonpb.Marshaler
	unmarshaler      jsonpb.Unmarshaler
	httpClient       *http.Client
}

func newSubscriberServer(
	projectCore project.ICore,
	brokerStore brokerstore.IBrokerStore,
	subscriptionCore subscription.ICore,
	topicCore topic.ICore,
	credentialCore credentials.ICore,
	replicaCount int,
	consumePlaneAddress string,
	config *httpclient.Config,
) *subscriberserver {
	consumeNodes := make(map[int]string, 0)
	for i := 0; i < replicaCount; i++ {
		consumeNodes[i] = fmt.Sprintf(consumePlaneAddress, consumePlaneDeployment, i, consumePlaneAddress)
	}
	marshaler := jsonpb.Marshaler{
		EnumsAsInts:  false,
		EmitDefaults: false,
		Indent:       "",
		OrigName:     false,
		AnyResolver:  nil,
	}

	unmarshaler := jsonpb.Unmarshaler{
		AllowUnknownFields: true,
		AnyResolver:        nil,
	}
	httpClient := httpclient.NewClient(config)
	return &subscriberserver{projectCore, brokerStore, subscriptionCore, topicCore, credentialCore, replicaCount, consumeNodes, marshaler, unmarshaler, httpClient}
}

// CreateSubscription to create a new subscription
func (s subscriberserver) CreateSubscription(ctx context.Context, req *metrov1.Subscription) (*metrov1.Subscription, error) {
	logger.Ctx(ctx).Infow("subscriberserver: received request to create subscription", "name", req.Name, "topic", req.Topic)
	span, ctx := opentracing.StartSpanFromContext(ctx, "SubscriberServer.CreateSubscription", opentracing.Tags{
		"topic":        req.Topic,
		"subscription": req.Name,
	})
	defer span.Finish()

	m, err := subscription.GetValidatedModelForCreate(ctx, req)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	err = s.subscriptionCore.CreateSubscription(ctx, m)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}
	return req, nil
}

// UpdateSubscription updates a given subscription
func (s subscriberserver) UpdateSubscription(ctx context.Context, req *metrov1.UpdateSubscriptionRequest) (*metrov1.Subscription, error) {
	logger.Ctx(ctx).Infow("subscriberserver: received request to update subscription", "name", req.Subscription.Name, "topic", req.Subscription.Topic)
	span, ctx := opentracing.StartSpanFromContext(ctx, "SubscriberServer.UpdateSubscription")
	defer span.Finish()

	if err := subscription.ValidateUpdateSubscriptionRequest(ctx, req); err != nil {
		return nil, merror.ToGRPCError(err)
	}

	sub, err := s.subscriptionCore.Get(ctx, req.Subscription.Name)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	existingSubscription := subscription.ModelToSubscriptionProtoV1(sub)
	fmutils.Prune(existingSubscription, req.UpdateMask.Paths)
	fmutils.Filter(req.Subscription, req.UpdateMask.Paths)

	if err = mergo.Merge(existingSubscription, req.Subscription); err != nil {
		return nil, merror.ToGRPCError(err)
	}

	patchedModel, err := subscription.GetValidatedModelForUpdate(ctx, existingSubscription)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	if err = s.subscriptionCore.UpdateSubscription(ctx, patchedModel); err != nil {
		return nil, merror.ToGRPCError(err)
	}
	patchedProto := subscription.ModelToSubscriptionProtoV1(patchedModel)
	return patchedProto, nil
}

// Acknowledge a message
func (s subscriberserver) Acknowledge(ctx context.Context, req *metrov1.AcknowledgeRequest) (*emptypb.Empty, error) {
	logger.Ctx(ctx).Infow("subscriberserver: received request to ack messages", "ack_req", req.String())

	span, ctx := opentracing.StartSpanFromContext(ctx, "SubscriberServer.Acknowledge", opentracing.Tags{
		"subscription": req.Subscription,
		"ack_ids":      req.AckIds,
	})
	defer span.Finish()

	parsedReq, parseErr := stream.NewParsedAcknowledgeRequest(req)
	if parseErr != nil {
		logger.Ctx(ctx).Errorw("subscriberserver: error is parsing ack request", "request", req, "error", parseErr.Error())
		return nil, merror.ToGRPCError(parseErr)
	}
	AckIdsByPartitions := make(map[int32][]string)
	for _, ack := range parsedReq.AckMessages {
		AckIdsByPartitions[ack.Partition] = append(AckIdsByPartitions[ack.Partition], ack.AckID)
	}

	for part, msgs := range AckIdsByPartitions {
		hash := s.subscriptionCore.FetchSubscriptionHash(ctx, req.Subscription, int(part))

		if node, ok := s.consumeNodes[hash%s.replicaCount]; ok {
			var b []byte
			byteBuffer := bytes.NewBuffer(b)
			body := &metrov1.AcknowledgeRequest{
				Subscription: req.Subscription,
				AckIds:       msgs,
			}
			err := s.marshaler.Marshal(byteBuffer, body)
			if err != nil {
				logger.Ctx(ctx).Errorw("subscriberserver: Failed to marshal upstream request", "error", err.Error(), "subscription", parsedReq.Subscription)
			}
			cpReq, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf(acknowledgeRequestFormat, node, parsedReq.Subscription), byteBuffer)

			_, err = s.httpClient.Do(cpReq)
			if err != nil {
				logger.Ctx(ctx).Errorw("subscriberserver: Failed to send request to consume plane", "error", err.Error())
			}

		} else {
			logger.Ctx(ctx).Errorw("subscriberserver: Failed to fetch consume plane for partition",
				"subscription", parsedReq.Subscription,
				"partition", part,
				"hash", hash,
				"replicaCount", s.replicaCount,
			)
			continue
		}

	}
	return new(emptypb.Empty), nil
}

// Pull messages
func (s subscriberserver) Pull(ctx context.Context, req *metrov1.PullRequest) (*metrov1.PullResponse, error) {
	logger.Ctx(ctx).Infow("subscriberserver: received request to pull messages", "pull_req", req.String())

	span, ctx := opentracing.StartSpanFromContext(ctx, "SubscriberServer.Pull", opentracing.Tags{
		"subscription": req.Subscription,
	})
	defer span.Finish()
	parsedReq, parseErr := stream.NewParsedPullRequest(req)
	if parseErr != nil {
		logger.Ctx(ctx).Errorw("subscriberserver: error is parsing pull request", "request", req, "error", parseErr.Error())
		return &metrov1.PullResponse{}, parseErr
	}

	// Restricting to one partition till dynamic re-partitioning is in place.
	// This helps us avoid two lookups for now (subscription lookup, topic lookup)
	hash := s.subscriptionCore.FetchSubscriptionHash(ctx, req.Subscription, 0)

	if node, ok := s.consumeNodes[hash%s.replicaCount]; ok {
		var b []byte
		byteBuffer := bytes.NewBuffer(b)
		body := &metrov1.FetchRequest{
			Subscription: parsedReq.Subscription,
			Partition:    0,
			MaxMessages:  int32(parsedReq.MaxMessages),
		}
		err := s.marshaler.Marshal(byteBuffer, body)
		if err != nil {
			logger.Ctx(ctx).Errorw("subscriberserver: Failed to marshal upstream request", "error", err.Error(), "subscription", parsedReq.Subscription)
		}

		cpReq, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf(fetchRequestFormat, node, parsedReq.Subscription), byteBuffer)
		if err != nil {
			logger.Ctx(ctx).Errorw("subscriberserver: Failed to create request object for consume plane", "error", err.Error(), "url", fmt.Sprintf(fetchRequestFormat, node, parsedReq.Subscription))
		}

		cpRes, err := s.httpClient.Do(cpReq)
		if err != nil {
			logger.Ctx(ctx).Errorw("subscriberserver: Failed to send request to consume plane", "error", err.Error(), "url", fmt.Sprintf(fetchRequestFormat, node, parsedReq.Subscription))
		}
		defer cpRes.Body.Close()
		if cpRes.StatusCode == http.StatusOK {
			resp := &metrov1.PullResponse{}

			err = s.unmarshaler.Unmarshal(cpRes.Body, resp)
			if err != nil {
				logger.Ctx(ctx).Errorw("subscriberserver: Failed to Unmarshal response", "error", err.Error())
			}
			return resp, nil

		}
	} else {
		logger.Ctx(ctx).Errorw("subscriberserver: Failed to fetch consume plane for partition",
			"subscription", parsedReq.Subscription,
			"hash", hash,
			"replicaCount", s.replicaCount,
		)

	}

	return &metrov1.PullResponse{}, nil
}

// StreamingPull ...
func (s subscriberserver) StreamingPull(server metrov1.Subscriber_StreamingPullServer) error {
	// TODO: Implement streaming pull with state management

	return http.ErrNotSupported
}

// DeleteSubscription deletes a subscription
func (s subscriberserver) DeleteSubscription(ctx context.Context, req *metrov1.DeleteSubscriptionRequest) (*emptypb.Empty, error) {
	logger.Ctx(ctx).Infow("subscriberserver: received request to delete subscription", "name", req.Subscription)

	span, ctx := opentracing.StartSpanFromContext(ctx, "SubscriberServer.DeleteSubscription", opentracing.Tags{
		"subscription": req.Subscription,
	})
	defer span.Finish()

	m, err := subscription.GetValidatedModelForDelete(ctx, &metrov1.Subscription{Name: req.Subscription})
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}

	err = s.subscriptionCore.DeleteSubscription(ctx, m)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s subscriberserver) ModifyAckDeadline(ctx context.Context, req *metrov1.ModifyAckDeadlineRequest) (*emptypb.Empty, error) {
	logger.Ctx(ctx).Infow("subscriberserver: received request to modack messages", "mod_ack_req", req.String())

	span, ctx := opentracing.StartSpanFromContext(ctx, "SubscriberServer.ModifyAckDeadline", opentracing.Tags{
		"subscription": req.Subscription,
		"ack_ids":      req.AckIds,
	})
	defer span.Finish()

	parsedReq, parseErr := stream.NewParsedModifyAckDeadlineRequest(req)
	if parseErr != nil {
		logger.Ctx(ctx).Errorw("subscriberserver: error is parsing modack request", "request", req, "error", parseErr.Error())
		return nil, merror.ToGRPCError(parseErr)
	}
	AckIdsByPartitions := make(map[int32][]string)
	for _, ack := range parsedReq.AckMessages {
		AckIdsByPartitions[ack.Partition] = append(AckIdsByPartitions[ack.Partition], ack.AckID)
	}

	for part, msgs := range AckIdsByPartitions {
		hash := s.subscriptionCore.FetchSubscriptionHash(ctx, parsedReq.Subscription, int(part))
		if node, ok := s.consumeNodes[hash%s.replicaCount]; ok {
			var b []byte
			byteBuffer := bytes.NewBuffer(b)
			body := &metrov1.ModifyAckDeadlineRequest{
				Subscription:       req.Subscription,
				AckIds:             msgs,
				AckDeadlineSeconds: req.AckDeadlineSeconds,
			}
			err := s.marshaler.Marshal(byteBuffer, body)
			if err != nil {
				logger.Ctx(ctx).Errorw("subscriberserver: Failed to marshal upstream request", "error", err.Error(), "subscription", parsedReq.Subscription)
			}
			cpReq, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf(modAckRequestFormat, node, parsedReq.Subscription), byteBuffer)

			_, err = s.httpClient.Do(cpReq)
			if err != nil {
				logger.Ctx(ctx).Errorw("subscriberserver: Failed to send request to consume plane", "error", err.Error())
			}

		} else {
			logger.Ctx(ctx).Errorw("subscriberserver: Failed to fetch consume plane for partition",
				"subscription", parsedReq.Subscription,
				"partition", part,
				"hash", hash,
				"replicaCount", s.replicaCount,
			)
			continue
		}

	}
	return new(emptypb.Empty), nil
}

//AuthFuncOverride - Override function called by the auth interceptor
func (s subscriberserver) AuthFuncOverride(ctx context.Context, fullMethodName string, req interface{}) (context.Context, error) {
	return authRequest(ctx, s.credentialCore, fullMethodName, req)
}
