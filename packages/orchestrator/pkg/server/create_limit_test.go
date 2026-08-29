//go:build linux

package server

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

func TestSandboxCreateConcurrencyLimitTracksSemaphore(t *testing.T) {
	t.Parallel()

	semaphore, err := utils.NewAdjustableSemaphore(16)
	require.NoError(t, err)
	server := &Server{startingSandboxes: semaphore}
	require.Equal(t, uint64(16), server.SandboxCreateConcurrencyLimit())

	require.NoError(t, semaphore.SetLimit(8))
	require.Equal(t, uint64(8), server.SandboxCreateConcurrencyLimit())
}
