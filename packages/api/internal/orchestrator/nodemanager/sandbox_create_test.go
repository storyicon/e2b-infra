package nodemanager

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

func TestSandboxCreate_QuarantinesAfterThreeConsecutiveUnavailable(t *testing.T) {
	t.Parallel()

	node := NewTestNode("node-unavailable", api.NodeStatusReady, 0, 4,
		WithSandboxCreateError(status.Error(codes.Unavailable, "connection timed out")))

	for range createUnavailableThreshold - 1 {
		_, err := node.SandboxCreate(t.Context(), &orchestrator.SandboxCreateRequest{})
		require.Error(t, err)
		assert.Equal(t, api.NodeStatusReady, node.Status())
		assert.True(t, node.CanAcceptNewRequests())
	}

	_, err := node.SandboxCreate(t.Context(), &orchestrator.SandboxCreateRequest{})
	require.Error(t, err)
	assert.Equal(t, api.NodeStatusUnhealthy, node.Status())
	assert.False(t, node.CanAcceptNewRequests())
	assert.Equal(t, uint32(createUnavailableThreshold), node.consecutiveCreateUnavailable.Load())

	// A complete successful sync writes the reported status and clears passive
	// create-failure evidence. Exercise those two recovery writes together;
	// Sync's success branch owns the same operations.
	node.setStatus(t.Context(), api.NodeStatusReady, time.Now())
	node.recordSuccessfulSync()
	assert.Equal(t, uint32(0), node.consecutiveCreateUnavailable.Load())
	assert.Equal(t, api.NodeStatusReady, node.Status())
	assert.True(t, node.CanAcceptNewRequests())
}

func TestSandboxCreate_ConcurrentUnavailableTrackingIsSafe(t *testing.T) {
	t.Parallel()

	node := NewTestNode("node-concurrent", api.NodeStatusReady, 0, 4,
		WithSandboxCreateError(status.Error(codes.Unavailable, "connection timed out")))
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			_, _ = node.SandboxCreate(t.Context(), &orchestrator.SandboxCreateRequest{})
		})
	}
	wg.Wait()

	assert.Equal(t, api.NodeStatusUnhealthy, node.Status())
	assert.False(t, node.CanAcceptNewRequests())
	assert.Equal(t, uint32(100), node.consecutiveCreateUnavailable.Load())
}

func TestSandboxCreate_SuccessResetsUnavailableSequence(t *testing.T) {
	t.Parallel()

	createCalls := 0
	node := NewTestNode("node-reset", api.NodeStatusReady, 0, 4)
	node.SetSandboxClient(&MockSandboxClientCustom{CreateFunc: func() error {
		createCalls++
		switch createCalls {
		case 1, 2, 4, 5:
			return status.Error(codes.Unavailable, "connection timed out")
		default:
			return nil
		}
	}})

	for range 6 {
		_, _ = node.SandboxCreate(t.Context(), &orchestrator.SandboxCreateRequest{})
	}

	assert.Equal(t, api.NodeStatusReady, node.Status())
	assert.True(t, node.CanAcceptNewRequests())
	assert.Equal(t, uint32(0), node.consecutiveCreateUnavailable.Load())
}

func TestSandboxCreate_ApplicationFailureDoesNotQuarantineNode(t *testing.T) {
	t.Parallel()

	node := NewTestNode("node-internal", api.NodeStatusReady, 0, 4,
		WithSandboxCreateError(status.Error(codes.Internal, "sandbox init failed")))

	_, err := node.SandboxCreate(t.Context(), &orchestrator.SandboxCreateRequest{})
	require.Error(t, err)
	assert.Equal(t, api.NodeStatusReady, node.Status())
	assert.True(t, node.CanAcceptNewRequests())
}
