package filter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCondition_Evaluate(t *testing.T) {
	f := &Condition{}

	tests := []struct {
		filterExpression string
		args             map[string]string
		expected         bool
	}{
		{
			filterExpression: "attributes.name=\"test\" OR (attributes.name=\"abc\" AND attributes.domain=\"com\")",
			args:             map[string]string{"name": "abc", "domain": "com"},
			expected:         true,
		},
		{
			filterExpression: "attributes.name!=\"test\" AND attributes.domain=\"com\"",
			args:             map[string]string{"name": "abc", "domain": "com"},
			expected:         true,
		},
		{
			filterExpression: "hasPrefix(attributes.name, \"abc\") AND NOT attributes:domain",
			args:             map[string]string{"name": "abcxyz", "domain": "abc"},
			expected:         false,
		},
	}

	for _, test := range tests {
		err := Parser.ParseString("", test.filterExpression, f)
		fmt.Println("filter expression:", test.filterExpression, " parsed ", f)
		assert.NoError(t, err)
		got, err := f.Evaluate(test.args)
		assert.NoError(t, err)
		assert.Equal(t, test.expected, got)
	}
}
