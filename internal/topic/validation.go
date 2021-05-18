package topic

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/razorpay/metro/internal/merror"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

var topicNameRegex *regexp.Regexp

func init() {
	var err error
	// https://github.com/googleapis/googleapis/blob/69697504d9eba1d064820c3085b4750767be6d08/google/pubsub/v1/pubsub.proto#L170
	// Note: check for project ID would happen while creating the project, hence not enforcing it here
	topicNameRegex, err = regexp.Compile("projects/(.*)/topics/([A-Za-z][A-Za-z0-9-_.~+%]{2,254})$")
	if err != nil {
		panic(err)
	}
}

// GetValidatedModel validates an incoming proto request and returns the model
func GetValidatedModel(ctx context.Context, req *metrov1.Topic) (*Model, error) {
	p, t, err := ExtractTopicMetaAndValidate(ctx, req.GetName())
	if err != nil {
		return nil, merror.Newf(merror.InvalidArgument, "invalid [topics] name: (name=%s)", req.Name)
	}
	m := &Model{}
	m.Name = req.GetName()
	m.Labels = req.GetLabels()
	m.ExtractedProjectID = p
	m.ExtractedTopicName = t

	partitions := DefaultNumPartitions
	if req.GetNumPartitions() > 0 {
		if int(req.GetNumPartitions()) > MaxAllowedNumPartitions {
			return nil, merror.Newf(merror.InvalidArgument, "max allowed partitions for a topic is [%v]", MaxAllowedNumPartitions)
		}
		partitions = int(req.GetNumPartitions())
	}
	m.NumPartitions = partitions

	return m, nil
}

// ExtractTopicMetaAndValidate extracts  topic metadata from its fully qualified name
func ExtractTopicMetaAndValidate(ctx context.Context, name string) (projectID string, topicName string, err error) {
	match := topicNameRegex.FindStringSubmatch(name)
	if len(match) != 3 {
		err = fmt.Errorf("invalid topic name")
		return "", "", err
	}
	projectID = topicNameRegex.FindStringSubmatch(name)[1]
	topicName = topicNameRegex.FindStringSubmatch(name)[2]
	if strings.HasPrefix(topicName, "goog") {
		err = fmt.Errorf("topic name cannot start with goog")
		return "", "", err
	}

	// -retry is reserved for internal retry topic
	if strings.HasSuffix(topicName, RetryTopicSuffix) {
		err = fmt.Errorf("topic name cannot end with " + RetryTopicSuffix)
		return "", "", err
	}

	if strings.HasSuffix(topicName, DeadLetterTopicSuffix) {
		err = fmt.Errorf("topic name cannot end with " + DeadLetterTopicSuffix)
		return "", "", err
	}

	return projectID, topicName, nil
}
