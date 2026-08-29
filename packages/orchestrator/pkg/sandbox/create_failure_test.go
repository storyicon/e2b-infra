//go:build linux

package sandbox

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJoinCreateAndCleanupErrors(t *testing.T) {
	t.Parallel()

	createErr := errors.New("create failed")
	cleanupErr := errors.New("cleanup failed")

	t.Run("successful cleanup preserves only the create failure", func(t *testing.T) {
		t.Parallel()

		err := JoinCreateAndCleanupErrors(createErr, nil)

		require.ErrorIs(t, err, createErr)
		require.NotErrorIs(t, err, ErrSandboxCleanupFailed)
	})

	t.Run("failed cleanup preserves both causes and marks cleanup failure", func(t *testing.T) {
		t.Parallel()

		err := JoinCreateAndCleanupErrors(createErr, cleanupErr)

		require.ErrorIs(t, err, createErr)
		require.ErrorIs(t, err, cleanupErr)
		require.ErrorIs(t, err, ErrSandboxCleanupFailed)
	})
}
