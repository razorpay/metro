package topic

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/razorpay/metro/internal/merror"

	"github.com/razorpay/metro/pkg/logger"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

// GetValidatedModel validates an incoming proto request and returns the model
func GetValidatedModel(ctx context.Context, req *metrov1.Topic) (*Model, error) {
	p, t, err := extractTopicMetaAndValidate(ctx, req.GetName())
	if err != nil {
		return nil, merror.Newf(merror.InvalidArgument, "Invalid [topics] name: (name=%s)", req.Name)
	}
	m := &Model{}
	m.Name = req.GetName()
	m.Labels = req.GetLabels()
	m.ExtractedProjectID = p
	m.ExtractedTopicName = t
	return m, nil
}

func extractTopicMetaAndValidate(ctx context.Context, name string) (projectID string, topicName string, err error) {
	// https://github.com/googleapis/googleapis/blob/69697504d9eba1d064820c3085b4750767be6d08/google/pubsub/v1/pubsub.proto#L170
	// Note: check for project ID would happen while creating the project, hence not enforcing it here
	r, err := regexp.Compile("projects/(.*)/topics/([A-Za-z][A-Za-z0-9-_.~+%]{2,254})$")
	if err != nil {
		logger.Ctx(ctx).Error(err.Error())
		return
	}
	match := r.FindStringSubmatch(name)
	if len(match) != 3 {
		err = fmt.Errorf("invalid topic name")
		logger.Ctx(ctx).Error(err.Error())
		return
	}
	projectID = r.FindStringSubmatch(name)[1]
	topicName = r.FindStringSubmatch(name)[2]
	if strings.HasPrefix(topicName, "goog") {
		err = fmt.Errorf("topic name cannot start with goog")
		logger.Ctx(ctx).Error(err.Error())
		return
	}
	return
}
