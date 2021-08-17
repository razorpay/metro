package web

import (
	"context"
	"testing"

	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/stretchr/testify/assert"
)

func TestGetProjectIDFromRequest_PublishRequest(t *testing.T) {
	ctx := context.Background()
	req := &metrov1.PublishRequest{
		Topic: "projects/project-p001/topics/topic-t001",
	}
	pid, err := getProjectIDFromRequest(ctx, req)
	assert.Equal(t, "project-p001", pid)
	assert.Nil(t, err)
}

func TestGetProjectIDFromRequest_DeleteTopicRequest(t *testing.T) {
	ctx := context.Background()
	req := &metrov1.DeleteTopicRequest{
		Topic: "projects/project-p001/topics/topic-t001",
	}
	pid, err := getProjectIDFromRequest(ctx, req)
	assert.Equal(t, "project-p001", pid)
	assert.Nil(t, err)
}

func TestGetProjectIDFromRequest_Topic(t *testing.T) {
	ctx := context.Background()
	req := &metrov1.Topic{
		Name: "projects/project-p001/topics/topic-t001",
	}
	pid, err := getProjectIDFromRequest(ctx, req)
	assert.Equal(t, "project-p001", pid)
	assert.Nil(t, err)
}

func TestGetProjectIDFromRequest_Subscription(t *testing.T) {
	ctx := context.Background()
	req := &metrov1.Subscription{
		Name: "projects/project-p001/subscriptions/subscription-s001",
	}
	pid, err := getProjectIDFromRequest(ctx, req)
	assert.Equal(t, "project-p001", pid)
	assert.Nil(t, err)
}

func TestGetProjectIDFromRequest_UpdateSubscriptionRequest(t *testing.T) {
	ctx := context.Background()
	req := &metrov1.UpdateSubscriptionRequest{
		Subscription: &metrov1.Subscription{
			Name: "projects/project-p001/subscriptions/subscription-s001",
		},
	}
	pid, err := getProjectIDFromRequest(ctx, req)
	assert.Equal(t, "project-p001", pid)
	assert.Nil(t, err)
}

func TestGetProjectIDFromRequest_AcknowledgeRequest(t *testing.T) {
	ctx := context.Background()
	req := &metrov1.AcknowledgeRequest{
		Subscription: "projects/project-p001/subscriptions/subscription-s001",
	}
	pid, err := getProjectIDFromRequest(ctx, req)
	assert.Equal(t, "project-p001", pid)
	assert.Nil(t, err)
}

func TestGetProjectIDFromRequest_PullRequest(t *testing.T) {
	ctx := context.Background()
	req := &metrov1.PullRequest{
		Subscription: "projects/project-p001/subscriptions/subscription-s001",
	}
	pid, err := getProjectIDFromRequest(ctx, req)
	assert.Equal(t, "project-p001", pid)
	assert.Nil(t, err)
}

func TestGetProjectIDFromRequest_DeleteSubscriptionRequest(t *testing.T) {
	ctx := context.Background()
	req := &metrov1.DeleteSubscriptionRequest{
		Subscription: "projects/project-p001/subscriptions/subscription-s001",
	}
	pid, err := getProjectIDFromRequest(ctx, req)
	assert.Equal(t, "project-p001", pid)
	assert.Nil(t, err)
}

func TestGetProjectIDFromRequest_ModifyAckDeadlineRequest(t *testing.T) {
	ctx := context.Background()
	req := &metrov1.ModifyAckDeadlineRequest{
		Subscription: "projects/project-p001/subscriptions/subscription-s001",
	}
	pid, err := getProjectIDFromRequest(ctx, req)
	assert.Equal(t, "project-p001", pid)
	assert.Nil(t, err)
}

func TestGetProjectIDFromRequest_UnknownType(t *testing.T) {
	ctx := context.Background()
	_, err := getProjectIDFromRequest(ctx, 5)
	assert.NotNil(t, err)
}

func TestGetProjectIDFromResourceName_InvalidResourceName(t *testing.T) {
	ctx := context.Background()
	_, err := getProjectIDFromResourceName(ctx, "invalid resource name")
	assert.NotNil(t, err)
}
