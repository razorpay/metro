// +build unit

package web

import (
	"context"
	"testing"

	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/stretchr/testify/assert"
)

func TestGetProjectIDFromRequest(t *testing.T) {
	type test struct {
		request interface{}
		pid     string
		err     error
	}
	tests := []test{
		{
			request: &metrov1.PublishRequest{
				Topic: "projects/project-p001/topics/topic-t001",
			},
			pid: "project-p001",
		}, {
			request: &metrov1.DeleteTopicRequest{
				Topic: "projects/project-p001/topics/topic-t001",
			},
			pid: "project-p001",
		}, {
			request: &metrov1.Topic{
				Name: "projects/project-p001/topics/topic-t001",
			},
			pid: "project-p001",
		}, {
			request: &metrov1.Subscription{
				Name: "projects/project-p001/subscriptions/subscription-s001",
			},
			pid: "project-p001",
		}, {
			request: &metrov1.UpdateSubscriptionRequest{
				Subscription: &metrov1.Subscription{
					Name: "projects/project-p001/subscriptions/subscription-s001",
				},
			},
			pid: "project-p001",
		}, {
			request: &metrov1.AcknowledgeRequest{
				Subscription: "projects/project-p001/subscriptions/subscription-s001",
			},
			pid: "project-p001",
		}, {
			request: &metrov1.PullRequest{
				Subscription: "projects/project-p001/subscriptions/subscription-s001",
			},
			pid: "project-p001",
		}, {
			request: &metrov1.DeleteSubscriptionRequest{
				Subscription: "projects/project-p001/subscriptions/subscription-s001",
			},
			pid: "project-p001",
		}, {
			request: &metrov1.ModifyAckDeadlineRequest{
				Subscription: "projects/project-p001/subscriptions/subscription-s001",
			},
			pid: "project-p001",
		}, {
			request: &metrov1.UpdateSubscriptionRequest{},
			pid:     "",
			err:     unknownResourceError,
		}, {
			request: nil,
			pid:     "",
			err:     unknownResourceError,
		}, {
			request: &metrov1.ModifyAckDeadlineRequest{
				Subscription: "subscriptions/subscription-s001",
			},
			pid: "",
			err: invalidResourceNameError,
		}, {
			request: &metrov1.ListTopicSubscriptionsRequest{
				Topic: "projects/project-p001/topics/topic-001",
			},
			pid: "project-p001",
		}, {
			request: &metrov1.ListProjectSubscriptionsRequest{
				ProjectId: "project-p001",
			},
			pid: "project-p001",
		}, {
			request: &metrov1.ListProjectTopicsRequest{
				ProjectId: "project-p001",
			},
			pid: "project-p001",
		},
	}

	ctx := context.Background()
	for _, tst := range tests {
		pid, err := getProjectIDFromRequest(ctx, tst.request)
		assert.Equal(t, tst.pid, pid)
		assert.Equal(t, tst.err, err)
	}
}

func Test_getResourceNameFromRequest(t *testing.T) {
	type args struct {
		ctx context.Context
		req interface{}
	}
	ctx := context.Background()
	type UnknownRequest struct{}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Get resourceName from GetSubscriptionRequest successfully",
			args: args{
				ctx: ctx,
				req: &metrov1.GetSubscriptionRequest{Name: "projects/project123/subscriptions/testsub"},
			},
			want:    "projects/project123/subscriptions/testsub",
			wantErr: false,
		},
		{
			name: "Throw error as unknown request type",
			args: args{
				ctx: ctx,
				req: &UnknownRequest{},
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getResourceNameFromRequest(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("getResourceNameFromRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("getResourceNameFromRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}
