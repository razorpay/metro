// +build unit

package leaderelection

import (
	"context"
	"github.com/magiconair/properties/assert"
	"testing"
)

func TestConfig(t *testing.T) {
	tests := []struct {
		config Config
		err    error
	}{
		{
			config: Config{
				Callbacks: LeaderCallbacks{
					OnStartedLeading: func(ctx context.Context) error {
						return nil
					},
					OnStoppedLeading: func(ctx context.Context) {

					},
				},
			},
			err: ErrInvalidLockPath,
		},
		{
			config: Config{
				LockPath: "test",
			},
			err: ErrInvalidOnStartedLeadingCallback,
		},
		{
			config: Config{
				LockPath: "test",
				Callbacks: LeaderCallbacks{
					OnStartedLeading: func(ctx context.Context) error {
						return nil
					},
				},
			},
			err: ErrInvalidOnStoppedLeadingCallback,
		},
		{
			config: Config{
				LockPath: "test",
				Callbacks: LeaderCallbacks{
					OnStartedLeading: func(ctx context.Context) error {
						return nil
					},
					OnStoppedLeading: func(ctx context.Context) {

					},
				},
			},
			err: nil,
		},
	}

	for i := range tests {
		ret := tests[i].config.Validate()
		assert.Equal(t, ret, tests[i].err)
	}
}
