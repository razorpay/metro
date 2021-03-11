package leaderelection

import (
	"context"
	"testing"
	"time"

	"github.com/magiconair/properties/assert"
)

func TestConfig(t *testing.T) {
	tests := []struct {
		config Config
		err    error
	}{
		{
			config: Config{
				LeaseDuration: 0 * time.Second,
				LockPath:      "test",
				Callbacks: LeaderCallbacks{
					OnStartedLeading: func(ctx context.Context) error {
						return nil
					},
					OnStoppedLeading: func() {

					},
				},
			},
			err: ErrInvalidLeaseDuration,
		},
		{
			config: Config{
				LeaseDuration: 10 * time.Second,
				Callbacks: LeaderCallbacks{
					OnStartedLeading: func(ctx context.Context) error {
						return nil
					},
					OnStoppedLeading: func() {

					},
				},
			},
			err: ErrInvalidLockPath,
		},
		{
			config: Config{
				LeaseDuration: 10 * time.Second,
				LockPath:      "test",
			},
			err: ErrInvalidOnStartedLeadingCallback,
		},
		{
			config: Config{
				LeaseDuration: 10 * time.Second,
				LockPath:      "test",
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
				LeaseDuration: 10 * time.Second,
				LockPath:      "test",
				Callbacks: LeaderCallbacks{
					OnStartedLeading: func(ctx context.Context) error {
						return nil
					},
					OnStoppedLeading: func() {

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
