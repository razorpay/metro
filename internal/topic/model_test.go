// +build unit

package topic

import (
	"testing"

	"github.com/razorpay/metro/internal/common"
	"github.com/stretchr/testify/assert"
)

func getDummyTopicModel() *Model {
	return &Model{
		Name:               "projects/test-project/topics/test-topic",
		Labels:             map[string]string{"label": "value"},
		ExtractedProjectID: "test-project",
		ExtractedTopicName: "test-topic",
		NumPartitions:      DefaultNumPartitions,
	}
}

func getDLQDummyTopicModel(name string) *Model {
	return &Model{
		Name:               "projects/test-project/topics/" + name,
		Labels:             map[string]string{"label": "value"},
		ExtractedProjectID: "test-project",
		ExtractedTopicName: name,
		NumPartitions:      DefaultNumPartitions,
	}
}

func TestModel_Prefix(t *testing.T) {
	dTopic := getDummyTopicModel()
	assert.Equal(t, common.GetBasePrefix()+Prefix+dTopic.ExtractedProjectID+"/", dTopic.Prefix())
}

func TestModel_Key(t *testing.T) {
	dTopic := getDummyTopicModel()
	assert.Equal(t, dTopic.Prefix()+dTopic.ExtractedTopicName, dTopic.Key())
}

func TestModel_IsSubscriptionInternalTopic(t *testing.T) {
	type fields struct {
		Name               string
		ExtractedTopicName string
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "check if topic is subscription's internal topic success",
			fields: fields{
				Name:               "projects/test-project/topics/test-topic-subscription-internal",
				ExtractedTopicName: "test-topic-subscription-internal",
			},
			want: true,
		},
		{
			name: "check if topic is subscription's internal topic failure",
			fields: fields{
				Name:               "projects/test-project/topics/test-topic",
				ExtractedTopicName: "test-topic",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Model{
				Name:               tt.fields.Name,
				ExtractedTopicName: tt.fields.ExtractedTopicName,
			}
			assert.Equalf(t, tt.want, m.IsSubscriptionInternalTopic(), "IsSubscriptionInternalTopic()")
		})
	}
}
