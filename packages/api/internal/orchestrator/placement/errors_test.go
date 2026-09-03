package placement

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	orchestratorgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

func TestPlaceSandbox_TimeoutReturnsTypedError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	nodes := []*nodemanager.Node{nodemanager.NewTestNode("node1", api.NodeStatusReady, 0, 4)}

	_, err := PlaceSandbox(ctx, failIfCalled(t), nodes, nil, testSbxRequest("test-sandbox"), CPURequirement{}, false, nil)

	var timeoutErr PlacementTimeoutError
	require.ErrorAs(t, err, &timeoutErr)
	assert.Equal(t, 0, timeoutErr.Attempts)
}

func TestPlaceSandbox_DeadlineDuringFinalAttemptReturnsTimeout(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	nodes := []*nodemanager.Node{
		nodemanager.NewTestNode("node1", api.NodeStatusReady, 0, 4, nodemanager.WithSandboxCreateError(status.Error(codes.Internal, "create failed on first node"))),
		nodemanager.NewTestNode("node2", api.NodeStatusReady, 0, 4, nodemanager.WithSandboxCreateError(status.Error(codes.Internal, "create failed on second node"))),
		nodemanager.NewTestNode("node3", api.NodeStatusReady, 0, 4),
	}
	nodes[2].SetSandboxClient(erroringClient(cancel, status.Error(codes.DeadlineExceeded, "context deadline exceeded")))

	algorithm := stubAlgorithm{choose: func(excluded map[string]struct{}) (*nodemanager.Node, error) {
		for _, n := range nodes {
			if _, ok := excluded[n.ID]; !ok {
				return n, nil
			}
		}

		return nil, errors.New("all nodes excluded")
	}}

	result, err := PlaceSandbox(ctx, algorithm, nodes, nil, testSbxRequest("test-sandbox"), CPURequirement{}, false, nil)

	var timeoutErr PlacementTimeoutError
	require.ErrorAs(t, err, &timeoutErr)
	assert.Equal(t, 3, timeoutErr.Attempts)
	assert.True(t, result.TimedOut)
}

func TestPlaceSandbox_NoNodesReturnsTypedError(t *testing.T) {
	t.Parallel()

	_, err := PlaceSandbox(t.Context(), failIfCalled(t), nil, nil, testSbxRequest("test-sandbox"), CPURequirement{}, false, nil)

	var noNodesErr NoNodesAvailableError
	require.ErrorAs(t, err, &noNodesErr)
}

func TestPlaceSandbox_CapacitySpikeToDeadlineClassifiedAsCapacity(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	node := nodemanager.NewTestNode("node1", api.NodeStatusReady, 0, 4)
	node.SetSandboxClient(erroringClient(cancel, status.Error(codes.ResourceExhausted, "no capacity")))

	result, err := PlaceSandbox(ctx, failIfCalled(t), []*nodemanager.Node{node}, node, testSbxRequest("test-sandbox"), CPURequirement{}, false, nil)

	var noNodesErr NoNodesAvailableError
	require.ErrorAs(t, err, &noNodesErr)
	assert.True(t, result.TimedOut)
}

func TestPlaceSandbox_HardFailureThenRefusalsToDeadlineStaysTimeout(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	hard := nodemanager.NewTestNode("node-hard", api.NodeStatusReady, 0, 4, nodemanager.WithSandboxCreateError(status.Error(codes.Internal, "create failed")))
	exhausted := nodemanager.NewTestNode("node-exhausted", api.NodeStatusReady, 0, 4)
	exhausted.SetSandboxClient(erroringClient(cancel, status.Error(codes.ResourceExhausted, "no capacity")))

	algorithm := stubAlgorithm{choose: func(map[string]struct{}) (*nodemanager.Node, error) {
		return exhausted, nil
	}}

	_, err := PlaceSandbox(ctx, algorithm, []*nodemanager.Node{hard, exhausted}, hard, testSbxRequest("test-sandbox"), CPURequirement{}, false, nil)

	var timeoutErr PlacementTimeoutError
	require.ErrorAs(t, err, &timeoutErr)
	assert.Equal(t, 1, timeoutErr.Attempts)
}

func TestPlaceSandbox_AllExcludedForwardsLastCreateError(t *testing.T) {
	t.Parallel()

	nodes := []*nodemanager.Node{
		nodemanager.NewTestNode("node1", api.NodeStatusReady, 0, 4, nodemanager.WithSandboxCreateError(status.Error(codes.FailedPrecondition, "sandbox files for 'abc' not found"))),
	}

	algorithm := stubAlgorithm{choose: func(excluded map[string]struct{}) (*nodemanager.Node, error) {
		for _, n := range nodes {
			if _, ok := excluded[n.ID]; !ok {
				return n, nil
			}
		}

		return nil, errors.New("all nodes excluded")
	}}

	_, err := PlaceSandbox(t.Context(), algorithm, nodes, nil, testSbxRequest("test-sandbox"), CPURequirement{}, false, nil)

	var createErr SandboxCreateError
	require.ErrorAs(t, err, &createErr)
	assert.Equal(t, 1, createErr.Attempts)
	assert.Equal(t, codes.FailedPrecondition, status.Code(createErr.LastErr))
	assert.Contains(t, createErr.LastErr.Error(), "sandbox files for 'abc' not found")
}

func TestPlaceSandbox_NoEligibleNodeReturnsTypedError(t *testing.T) {
	t.Parallel()

	nodes := []*nodemanager.Node{nodemanager.NewTestNode("node1", api.NodeStatusReady, 0, 4)}
	algorithm := stubAlgorithm{choose: func(map[string]struct{}) (*nodemanager.Node, error) {
		return nil, FailedToPlaceSandboxError{}
	}}

	_, err := PlaceSandbox(t.Context(), algorithm, nodes, nil, testSbxRequest("test-sandbox"), CPURequirement{}, false, nil)

	var noEligibleErr FailedToPlaceSandboxError
	require.ErrorAs(t, err, &noEligibleErr)
}

func TestPlaceSandbox_RetriesExhaustedKeepsLastError(t *testing.T) {
	t.Parallel()

	nodes := []*nodemanager.Node{
		nodemanager.NewTestNode("node1", api.NodeStatusReady, 0, 4, nodemanager.WithSandboxCreateError(status.Error(codes.Internal, "create failed on first node"))),
		nodemanager.NewTestNode("node2", api.NodeStatusReady, 0, 4, nodemanager.WithSandboxCreateError(status.Error(codes.Internal, "create failed on second node"))),
		nodemanager.NewTestNode("node3", api.NodeStatusReady, 0, 4, nodemanager.WithSandboxCreateError(status.Error(codes.FailedPrecondition, "sandbox files for 'abc' not found"))),
	}

	algorithm := stubAlgorithm{choose: func(excluded map[string]struct{}) (*nodemanager.Node, error) {
		for _, n := range nodes {
			if _, ok := excluded[n.ID]; !ok {
				return n, nil
			}
		}

		return nil, errors.New("all nodes excluded")
	}}

	_, err := PlaceSandbox(t.Context(), algorithm, nodes, nil, testSbxRequest("test-sandbox"), CPURequirement{}, false, nil)

	var createErr SandboxCreateError
	require.ErrorAs(t, err, &createErr)
	assert.Equal(t, maxRetries, createErr.Attempts)
	assert.Equal(t, codes.FailedPrecondition, status.Code(createErr.LastErr))
	assert.Contains(t, createErr.LastErr.Error(), "sandbox files for 'abc' not found")
}

func TestPlaceSandbox_ChooseFailureAfterCreateAttemptForwardsCreateError(t *testing.T) {
	t.Parallel()

	failing := nodemanager.NewTestNode("node-failing", api.NodeStatusReady, 0, 4,
		nodemanager.WithSandboxCreateError(status.Error(codes.FailedPrecondition, "sandbox files for 'sbx-1' not found")))
	notReady := nodemanager.NewTestNode("node-draining", api.NodeStatusDraining, 0, 4)

	algorithm := stubAlgorithm{choose: func(excluded map[string]struct{}) (*nodemanager.Node, error) {
		if _, ok := excluded[failing.ID]; !ok {
			return failing, nil
		}

		return nil, FailedToPlaceSandboxError{}
	}}

	_, err := PlaceSandbox(t.Context(), algorithm, []*nodemanager.Node{failing, notReady}, nil, testSbxRequest("sbx-1"), CPURequirement{}, false, nil)

	var createErr SandboxCreateError
	require.ErrorAs(t, err, &createErr)
	assert.Equal(t, 1, createErr.Attempts)
	assert.Equal(t, codes.FailedPrecondition, status.Code(createErr.LastErr))
	assert.Contains(t, createErr.LastErr.Error(), "sandbox files for 'sbx-1' not found")
}

func TestPlaceSandbox_GuestReadinessRetryIsBoundedToTwoAttempts(t *testing.T) {
	t.Parallel()

	retryableTimeout := orchestratorgrpc.NewSandboxCreateError(
		codes.DeadlineExceeded,
		orchestratorgrpc.SandboxGuestReadinessTimeoutReason,
		"guest readiness timed out",
		true,
	)
	createCalls := 0
	nodes := make([]*nodemanager.Node, 3)
	for i := range nodes {
		nodes[i] = nodemanager.NewTestNode(
			fmt.Sprintf("node-%d", i),
			api.NodeStatusReady,
			0,
			4,
			nodemanager.WithSandboxCreateError(retryableTimeout),
		)
	}
	algorithm := stubAlgorithm{choose: func(excluded map[string]struct{}) (*nodemanager.Node, error) {
		for _, node := range nodes {
			if _, found := excluded[node.ID]; !found {
				createCalls++

				return node, nil
			}
		}

		return nil, errors.New("all nodes excluded")
	}}

	_, err := PlaceSandbox(t.Context(), algorithm, nodes, nil, testSbxRequest("sbx-guest-timeout"), CPURequirement{}, false, nil)

	var createErr SandboxCreateError
	require.ErrorAs(t, err, &createErr)
	require.Equal(t, 2, createErr.Attempts)
	require.Equal(t, 2, createCalls)
}

func TestPlaceSandbox_DoesNotGrantGuestBudgetToIncompleteDetails(t *testing.T) {
	t.Parallel()

	tests := map[string]error{
		"missing details": status.Error(codes.DeadlineExceeded, "guest readiness timed out"),
		"wrong reason": orchestratorgrpc.NewSandboxCreateError(
			codes.DeadlineExceeded,
			"SOME_OTHER_TIMEOUT",
			"guest readiness timed out",
			true,
		),
		"not retry safe": orchestratorgrpc.NewSandboxCreateError(
			codes.DeadlineExceeded,
			orchestratorgrpc.SandboxGuestReadinessTimeoutReason,
			"guest readiness timed out",
			false,
		),
	}

	for name, rpcErr := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			nodes := []*nodemanager.Node{
				nodemanager.NewTestNode("node-1", api.NodeStatusReady, 0, 4, nodemanager.WithSandboxCreateError(rpcErr)),
				nodemanager.NewTestNode("node-2", api.NodeStatusReady, 0, 4, nodemanager.WithSandboxCreateError(rpcErr)),
				nodemanager.NewTestNode("node-3", api.NodeStatusReady, 0, 4, nodemanager.WithSandboxCreateError(rpcErr)),
			}
			algorithm := stubAlgorithm{choose: func(excluded map[string]struct{}) (*nodemanager.Node, error) {
				for _, node := range nodes {
					if _, found := excluded[node.ID]; !found {
						return node, nil
					}
				}

				return nil, errors.New("all nodes excluded")
			}}

			_, err := PlaceSandbox(t.Context(), algorithm, nodes, nil, testSbxRequest("sbx-compat"), CPURequirement{}, false, nil)

			var createErr SandboxCreateError
			require.ErrorAs(t, err, &createErr)
			require.Equal(t, maxRetries, createErr.Attempts, "must preserve the existing generic hard-failure policy")
		})
	}
}

func TestPlaceSandbox_CleanupFailureIsTerminal(t *testing.T) {
	t.Parallel()

	cleanupFailed := orchestratorgrpc.NewSandboxCreateError(
		codes.Internal,
		orchestratorgrpc.SandboxCreateCleanupFailedReason,
		"cleanup failed",
		false,
	)
	nodes := []*nodemanager.Node{
		nodemanager.NewTestNode("node-1", api.NodeStatusReady, 0, 4, nodemanager.WithSandboxCreateError(cleanupFailed)),
		nodemanager.NewTestNode("node-2", api.NodeStatusReady, 0, 4),
	}
	chooseCalls := 0
	algorithm := stubAlgorithm{choose: func(map[string]struct{}) (*nodemanager.Node, error) {
		chooseCalls++

		return nodes[chooseCalls-1], nil
	}}

	_, err := PlaceSandbox(t.Context(), algorithm, nodes, nil, testSbxRequest("sbx-cleanup-failed"), CPURequirement{}, false, nil)

	var createErr SandboxCreateError
	require.ErrorAs(t, err, &createErr)
	require.Equal(t, 1, createErr.Attempts)
	require.Equal(t, 1, chooseCalls)
}
