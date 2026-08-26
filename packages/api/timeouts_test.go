package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDeriveServerTimeouts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		capacityWait time.Duration
		request      time.Duration
		write        time.Duration
		shutdown     time.Duration
	}{
		{
			name:     "capacity waiting disabled",
			request:  70 * time.Second,
			write:    75 * time.Second,
			shutdown: 75 * time.Second,
		},
		{
			name:         "short capacity wait keeps defaults",
			capacityWait: 30 * time.Second,
			request:      70 * time.Second,
			write:        75 * time.Second,
			shutdown:     75 * time.Second,
		},
		{
			name:         "capacity wait extends the enclosing deadlines",
			capacityWait: 120 * time.Second,
			request:      125 * time.Second,
			write:        130 * time.Second,
			shutdown:     130 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			timeouts := deriveServerTimeouts(tt.capacityWait)

			require.Equal(t, tt.request, timeouts.request)
			require.Equal(t, tt.write, timeouts.write)
			require.Equal(t, tt.shutdown, timeouts.shutdown)
		})
	}
}
