package placement

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

type mockAlgorithm struct {
	mock.Mock
}

type contextBlockingSandboxClient struct {
	orchestrator.SandboxServiceClient

	started chan struct{}
}

func (c *contextBlockingSandboxClient) Create(ctx context.Context, _ *orchestrator.SandboxCreateRequest, _ ...grpc.CallOption) (*orchestrator.SandboxCreateResponse, error) {
	close(c.started)
	<-ctx.Done()

	return nil, status.FromContextError(ctx.Err()).Err()
}

func TestNodeExhaustedLogLevel(t *testing.T) {
	t.Parallel()

	require.Equal(t, zap.DebugLevel, nodeExhaustedLogLevel)
}

func (m *mockAlgorithm) chooseNode(ctx context.Context, nodes []*nodemanager.Node, nodesExcluded map[string]struct{}, requested nodemanager.SandboxResources, cpu CPURequirement, features FeatureRequirement, filterByLabels bool, requiredLabels []string) (*nodemanager.Node, error) {
	args := m.Called(ctx, nodes, nodesExcluded, requested, cpu, features, filterByLabels, requiredLabels)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*nodemanager.Node), args.Error(1)
}

func TestPlaceSandbox_SuccessfulPlacement(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	// Create test nodes
	node1 := nodemanager.NewTestNode("node1", api.NodeStatusReady, 3, 4)
	node2 := nodemanager.NewTestNode("node2", api.NodeStatusReady, 5, 4)
	node3 := nodemanager.NewTestNode("node3", api.NodeStatusReady, 7, 4)
	nodes := []*nodemanager.Node{node1, node2, node3}

	// Create a mock algorithm that returns node2
	algorithm := &mockAlgorithm{}
	algorithm.On("chooseNode", mock.Anything, nodes, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(node2, nil)

	sbxRequest := &orchestrator.SandboxCreateRequest{
		Sandbox: &orchestrator.SandboxConfig{
			SandboxId: "test-sandbox",
			Vcpu:      2,
			RamMb:     1024,
		},
	}

	resultNode, err := PlaceSandbox(ctx, algorithm, nodes, nil, sbxRequest, CPURequirement{}, false, nil)

	require.NoError(t, err)
	assert.NotNil(t, resultNode.Node)
	assert.Equal(t, node2, resultNode.Node)
	algorithm.AssertExpectations(t)
}

func TestPlaceSandboxSelectsAnotherNodeWhenCreateReservationsAreFull(t *testing.T) {
	t.Parallel()

	full := nodemanager.NewTestNode("full", api.NodeStatusReady, 0, 4)
	available := nodemanager.NewTestNode("available", api.NodeStatusReady, 0, 4)
	full.PlacementMetrics.SetCreateConcurrencyLimit(1)
	available.PlacementMetrics.SetCreateConcurrencyLimit(1)
	require.True(t, full.PlacementMetrics.TryReserve("existing", nodemanager.SandboxResources{}))

	var fullCreates atomic.Uint32
	full.SetSandboxClient(&nodemanager.MockSandboxClientCustom{CreateFunc: func() error {
		fullCreates.Add(1)

		return nil
	}})

	nodes := []*nodemanager.Node{full, available}
	algorithm := &mockAlgorithm{}
	algorithm.On("chooseNode", mock.Anything, nodes, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(full, nil).Once()
	algorithm.On("chooseNode", mock.Anything, nodes, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(available, nil).Once()

	result, err := PlaceSandbox(t.Context(), algorithm, nodes, nil, testSbxRequest("new-sandbox"), CPURequirement{}, false, nil)
	require.NoError(t, err)
	require.Equal(t, available, result.Node)
	require.Zero(t, fullCreates.Load(), "a full local reservation must prevent the create RPC")
	algorithm.AssertExpectations(t)
}

func TestPlaceSandboxReturnsCapacityWhenAllCreateReservationsAreFull(t *testing.T) {
	t.Parallel()

	first := nodemanager.NewTestNode("first", api.NodeStatusReady, 0, 4)
	second := nodemanager.NewTestNode("second", api.NodeStatusReady, 0, 4)
	for _, node := range []*nodemanager.Node{first, second} {
		node.PlacementMetrics.SetCreateConcurrencyLimit(1)
		require.True(t, node.PlacementMetrics.TryReserve("existing-"+node.ID, nodemanager.SandboxResources{}))
	}

	var createCalls atomic.Uint32
	client := &nodemanager.MockSandboxClientCustom{CreateFunc: func() error {
		createCalls.Add(1)

		return nil
	}}
	first.SetSandboxClient(client)
	second.SetSandboxClient(client)

	nodes := []*nodemanager.Node{first, second}
	algorithm := &mockAlgorithm{}
	algorithm.On("chooseNode", mock.Anything, nodes, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(first, nil).Once()
	algorithm.On("chooseNode", mock.Anything, nodes, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(second, nil).Once()

	result, err := PlaceSandboxOncePerNode(t.Context(), algorithm, nodes, nil, testSbxRequest("new-sandbox"), CPURequirement{}, false, nil)
	require.Error(t, err)
	var noNodes NoNodesAvailableError
	require.ErrorAs(t, err, &noNodes)
	require.Nil(t, result.Node)
	require.Zero(t, createCalls.Load())
	algorithm.AssertExpectations(t)
}

func TestPlaceSandboxPreferredNodeCannotBypassCreateReservation(t *testing.T) {
	t.Parallel()

	preferred := nodemanager.NewTestNode("preferred", api.NodeStatusReady, 0, 4)
	available := nodemanager.NewTestNode("available", api.NodeStatusReady, 0, 4)
	preferred.PlacementMetrics.SetCreateConcurrencyLimit(1)
	available.PlacementMetrics.SetCreateConcurrencyLimit(1)
	require.True(t, preferred.PlacementMetrics.TryReserve("existing", nodemanager.SandboxResources{}))

	var preferredCreates atomic.Uint32
	preferred.SetSandboxClient(&nodemanager.MockSandboxClientCustom{CreateFunc: func() error {
		preferredCreates.Add(1)

		return nil
	}})

	nodes := []*nodemanager.Node{preferred, available}
	algorithm := &mockAlgorithm{}
	algorithm.On("chooseNode", mock.Anything, nodes, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(available, nil).Once()

	result, err := PlaceSandbox(t.Context(), algorithm, nodes, preferred, testSbxRequest("new-sandbox"), CPURequirement{}, false, nil)
	require.NoError(t, err)
	require.Equal(t, available, result.Node)
	require.Zero(t, preferredCreates.Load())
	algorithm.AssertExpectations(t)
}

func TestPlaceSandboxReleasesCreateReservationOnEveryTerminalOutcome(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		node := nodemanager.NewTestNode("success", api.NodeStatusReady, 0, 4)
		node.PlacementMetrics.SetCreateConcurrencyLimit(1)
		algorithm := &mockAlgorithm{}
		algorithm.On("chooseNode", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(node, nil).Once()

		_, err := PlaceSandbox(t.Context(), algorithm, []*nodemanager.Node{node}, nil, testSbxRequest("sandbox"), CPURequirement{}, false, nil)
		require.NoError(t, err)
		require.Zero(t, node.PlacementMetrics.InProgressCount())
		require.True(t, node.PlacementMetrics.TryReserve("next", nodemanager.SandboxResources{}))
	})

	t.Run("failure", func(t *testing.T) {
		t.Parallel()

		node := nodemanager.NewTestNode("failure", api.NodeStatusReady, 0, 4, nodemanager.WithSandboxCreateError(status.Error(codes.Internal, "create failed")))
		node.PlacementMetrics.SetCreateConcurrencyLimit(1)
		algorithm := &mockAlgorithm{}
		algorithm.On("chooseNode", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(node, nil).Once()

		_, err := PlaceSandbox(t.Context(), algorithm, []*nodemanager.Node{node}, nil, testSbxRequest("sandbox"), CPURequirement{}, false, nil)
		require.Error(t, err)
		require.Zero(t, node.PlacementMetrics.InProgressCount())
		require.True(t, node.PlacementMetrics.TryReserve("next", nodemanager.SandboxResources{}))
	})

	t.Run("cancellation", func(t *testing.T) {
		t.Parallel()

		node := nodemanager.NewTestNode("cancelled", api.NodeStatusReady, 0, 4)
		node.PlacementMetrics.SetCreateConcurrencyLimit(1)
		started := make(chan struct{})
		node.SetSandboxClient(&contextBlockingSandboxClient{started: started})
		algorithm := &mockAlgorithm{}
		algorithm.On("chooseNode", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(node, nil).Once()

		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)
		go func() {
			_, err := PlaceSandbox(ctx, algorithm, []*nodemanager.Node{node}, nil, testSbxRequest("sandbox"), CPURequirement{}, false, nil)
			result <- err
		}()

		<-started
		cancel()
		require.Error(t, <-result)
		require.Zero(t, node.PlacementMetrics.InProgressCount())
		require.True(t, node.PlacementMetrics.TryReserve("next", nodemanager.SandboxResources{}))
	})
}

func TestPlaceSandbox_WithPreferredNode(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	// Create test nodes
	node1 := nodemanager.NewTestNode("node1", api.NodeStatusReady, 3, 4)
	node2 := nodemanager.NewTestNode("node2", api.NodeStatusReady, 5, 4)
	node3 := nodemanager.NewTestNode("node3", api.NodeStatusReady, 7, 4)
	nodes := []*nodemanager.Node{node1, node2, node3}

	sbxRequest := &orchestrator.SandboxCreateRequest{
		Sandbox: &orchestrator.SandboxConfig{
			SandboxId: "test-sandbox",
			Vcpu:      2,
			RamMb:     1024,
		},
	}

	// Test without preferred node - algorithm should be called
	algorithm := &mockAlgorithm{}
	algorithm.On("chooseNode", mock.Anything, nodes, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(node1, nil).Once()

	resultNode, err := PlaceSandbox(ctx, algorithm, nodes, nil, sbxRequest, CPURequirement{}, false, nil)
	require.NoError(t, err)
	assert.NotNil(t, resultNode.Node)
	assert.Equal(t, node1, resultNode.Node)
	algorithm.AssertExpectations(t)

	// Test with preferred node - should use the preferred node directly without calling algorithm
	resultNode, err = PlaceSandbox(ctx, algorithm, nodes, node2, sbxRequest, CPURequirement{}, false, nil)
	require.NoError(t, err)
	assert.NotNil(t, resultNode.Node)
	assert.Equal(t, node2, resultNode.Node)
	// Algorithm should not be called when preferred node is provided
	algorithm.AssertNotCalled(t, "chooseNode")
}

func TestPlaceSandbox_ContextTimeout(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Millisecond)
	defer cancel()

	algorithm := &mockAlgorithm{}
	algorithm.On("chooseNode", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ mock.Arguments) {
			// Simulate slow node selection
			time.Sleep(10 * time.Millisecond)
		}).
		Return(nil, errors.New("timeout"))

	sbxRequest := &orchestrator.SandboxCreateRequest{
		Sandbox: &orchestrator.SandboxConfig{
			SandboxId: "test-sandbox",
			Vcpu:      2,
			RamMb:     1024,
		},
	}

	resultNode, err := PlaceSandbox(ctx, algorithm, []*nodemanager.Node{
		nodemanager.NewTestNode("node1", api.NodeStatusReady, 3, 4),
	}, nil, sbxRequest, CPURequirement{}, false, nil)

	require.Error(t, err)
	assert.Nil(t, resultNode.Node)
	// The error could be either "timeout" from the algorithm or "request timed out" from ctx.Done()
	assert.True(t, err.Error() == "timeout" || strings.Contains(err.Error(), "request timed out"))
}

func TestPlaceSandbox_NoNodes(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	algorithm := &mockAlgorithm{}
	sbxRequest := &orchestrator.SandboxCreateRequest{
		Sandbox: &orchestrator.SandboxConfig{
			SandboxId: "test-sandbox",
			Vcpu:      2,
			RamMb:     1024,
		},
	}

	resultNode, err := PlaceSandbox(ctx, algorithm, []*nodemanager.Node{}, nil, sbxRequest, CPURequirement{}, false, nil)

	require.Error(t, err)
	assert.Nil(t, resultNode.Node)
	assert.Contains(t, err.Error(), "no nodes available")
}

func TestPlaceSandbox_AllNodesExcluded(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	algorithm := &mockAlgorithm{}
	algorithm.On("chooseNode", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("no nodes available"))

	sbxRequest := &orchestrator.SandboxCreateRequest{
		Sandbox: &orchestrator.SandboxConfig{
			SandboxId: "test-sandbox",
			Vcpu:      2,
			RamMb:     1024,
		},
	}

	resultNode, err := PlaceSandbox(ctx, algorithm, []*nodemanager.Node{
		nodemanager.NewTestNode("node1", api.NodeStatusReady, 3, 4),
	}, nil, sbxRequest, CPURequirement{}, false, nil)

	require.Error(t, err)
	assert.Nil(t, resultNode.Node)
	assert.Contains(t, err.Error(), "no nodes available")
	algorithm.AssertExpectations(t)
}

func TestPlaceSandbox_ResourceExhausted(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	// Create test nodes - node1 will return ResourceExhausted, node2 will succeed
	node1 := nodemanager.NewTestNode("node1", api.NodeStatusReady, 3, 4,
		nodemanager.WithSandboxCreateError(status.Error(codes.ResourceExhausted, "node exhausted")))
	node2 := nodemanager.NewTestNode("node2", api.NodeStatusReady, 5, 4)
	nodes := []*nodemanager.Node{node1, node2}

	// Algorithm should be called twice - first returns node1 (exhausted), then node2 (succeeds)
	algorithm := &mockAlgorithm{}
	algorithm.On("chooseNode", mock.Anything, nodes, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(node1, nil).Once()
	algorithm.On("chooseNode", mock.Anything, nodes, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(node2, nil).Once()

	sbxRequest := &orchestrator.SandboxCreateRequest{
		Sandbox: &orchestrator.SandboxConfig{
			SandboxId: "test-sandbox",
			Vcpu:      2,
			RamMb:     1024,
		},
	}

	resultNode, err := PlaceSandbox(ctx, algorithm, nodes, nil, sbxRequest, CPURequirement{}, false, nil)

	require.NoError(t, err)
	assert.NotNil(t, resultNode.Node)
	assert.Equal(t, node2, resultNode.Node, "should succeed on node2 after node1 was exhausted")
	algorithm.AssertExpectations(t)

	// Verify placement continued to node2 after node1 refused capacity.
	algorithm.AssertNumberOfCalls(t, "chooseNode", 2)
}

func TestPlaceSandbox_EachExhaustedNodeIsTriedOnce(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	nodes := []*nodemanager.Node{
		nodemanager.NewTestNode("node1", api.NodeStatusReady, 0, 4,
			nodemanager.WithSandboxCreateError(status.Error(codes.ResourceExhausted, "node exhausted"))),
		nodemanager.NewTestNode("node2", api.NodeStatusReady, 0, 4,
			nodemanager.WithSandboxCreateError(status.Error(codes.ResourceExhausted, "node exhausted"))),
	}

	chooseCalls := 0
	algorithm := stubAlgorithm{choose: func(excluded map[string]struct{}) (*nodemanager.Node, error) {
		chooseCalls++
		if chooseCalls > len(nodes) {
			cancel()

			return nodes[0], nil
		}

		for _, node := range nodes {
			if _, found := excluded[node.ID]; !found {
				return node, nil
			}
		}

		return nil, errors.New("all nodes exhausted")
	}}

	result, err := PlaceSandboxOncePerNode(
		ctx,
		algorithm,
		nodes,
		nil,
		testSbxRequest("test-sandbox"),
		CPURequirement{},
		false,
		nil,
	)

	var noNodesErr NoNodesAvailableError
	require.ErrorAs(t, err, &noNodesErr)
	assert.False(t, result.TimedOut)
	assert.Equal(t, len(nodes), chooseCalls)
}

func TestPlaceSandbox_DefaultRetriesExhaustedNode(t *testing.T) {
	t.Parallel()

	node := nodemanager.NewTestNode("node1", api.NodeStatusReady, 0, 4)
	createCalls := 0
	node.SetSandboxClient(&nodemanager.MockSandboxClientCustom{CreateFunc: func() error {
		createCalls++
		if createCalls == 1 {
			return status.Error(codes.ResourceExhausted, "temporarily exhausted")
		}

		return nil
	}})
	algorithm := stubAlgorithm{choose: func(map[string]struct{}) (*nodemanager.Node, error) {
		return node, nil
	}}

	result, err := PlaceSandbox(
		t.Context(),
		algorithm,
		[]*nodemanager.Node{node},
		nil,
		testSbxRequest("test-sandbox"),
		CPURequirement{},
		false,
		nil,
	)

	require.NoError(t, err)
	require.Same(t, node, result.Node)
	require.Equal(t, 2, createCalls)
}

func TestPlaceSandbox_TriggersOptimisticUpdate(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	// Create a node and record the initial allocated CPU
	node1 := nodemanager.NewTestNode("node1", api.NodeStatusReady, 0, 4)
	initialCpuAllocated := node1.Metrics().CpuAllocated

	nodes := []*nodemanager.Node{node1}

	// Mock algorithm directly returns node1
	algorithm := &mockAlgorithm{}
	algorithm.On("chooseNode", mock.Anything, nodes, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(node1, nil)

	// Request 2 vCPUs
	sbxRequest := &orchestrator.SandboxCreateRequest{
		Sandbox: &orchestrator.SandboxConfig{
			SandboxId: "test-optimistic-sandbox",
			Vcpu:      2,
			RamMb:     1024,
		},
	}

	resultNode, err := PlaceSandbox(ctx, algorithm, nodes, nil, sbxRequest, CPURequirement{}, false, nil)

	require.NoError(t, err)
	assert.NotNil(t, resultNode.Node)

	// Verify: After successful placement, the node's CpuAllocated should be increased by 2 from the base
	updatedCpuAllocated := resultNode.Node.Metrics().CpuAllocated
	assert.Equal(t, initialCpuAllocated+2, updatedCpuAllocated, "Node metrics should be optimistically updated after placement")
}
