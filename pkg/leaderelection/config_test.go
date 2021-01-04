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
				LeaseDuration: 3 * time.Second,
				RenewDeadline: 5 * time.Second,
				RetryPeriod:   3 * time.Second,
				Path:          "test",
				Callbacks: LeaderCallbacks{
					OnStartedLeading: func(ctx context.Context) {

					},
					OnStoppedLeading: func() {

					},
				},
			},
			err: ErrLeaseDurationLessThanRenewDeadline,
		},
		{
			config: Config{
				LeaseDuration: 0 * time.Second,
				RenewDeadline: -1 * time.Second,
				RetryPeriod:   3 * time.Second,
				Path:          "test",
				Callbacks: LeaderCallbacks{
					OnStartedLeading: func(ctx context.Context) {

					},
					OnStoppedLeading: func() {

					},
				},
			},
			err: ErrInvalidLeaseDuration,
		},
		{
			config: Config{
				LeaseDuration: 1 * time.Second,
				RenewDeadline: 0 * time.Second,
				RetryPeriod:   3 * time.Second,
				Path:          "test",
				Callbacks: LeaderCallbacks{
					OnStartedLeading: func(ctx context.Context) {

					},
					OnStoppedLeading: func() {

					},
				},
			},
			err: ErrInvalidRenewDeadline,
		},
		{
			config: Config{
				LeaseDuration: 10 * time.Second,
				RenewDeadline: 5 * time.Second,
				RetryPeriod:   0 * time.Second,
				Path:          "test",
				Callbacks: LeaderCallbacks{
					OnStartedLeading: func(ctx context.Context) {

					},
					OnStoppedLeading: func() {

					},
				},
			},
			err: ErrInvalidRetryPeriod,
		},
		{
			config: Config{
				LeaseDuration: 10 * time.Second,
				RenewDeadline: 5 * time.Second,
				RetryPeriod:   1 * time.Second,
				Callbacks: LeaderCallbacks{
					OnStartedLeading: func(ctx context.Context) {

					},
					OnStoppedLeading: func() {

					},
				},
			},
			err: ErrInvalidPath,
		},
		{
			config: Config{
				LeaseDuration: 10 * time.Second,
				RenewDeadline: 5 * time.Second,
				RetryPeriod:   1 * time.Second,
				Path:          "test",
			},
			err: ErrInvalidOnStartedLeadingCallback,
		},
		{
			config: Config{
				LeaseDuration: 10 * time.Second,
				RenewDeadline: 5 * time.Second,
				RetryPeriod:   1 * time.Second,
				Path:          "test",
				Callbacks: LeaderCallbacks{
					OnStartedLeading: func(ctx context.Context) {

					},
				},
			},
			err: ErrInvalidOnStoppedLeadingCallback,
		},
		{
			config: Config{
				LeaseDuration: 10 * time.Second,
				RenewDeadline: 5 * time.Second,
				RetryPeriod:   1 * time.Second,
				Path:          "test",
				Callbacks: LeaderCallbacks{
					OnStartedLeading: func(ctx context.Context) {

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
