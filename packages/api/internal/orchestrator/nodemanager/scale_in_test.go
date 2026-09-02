package nodemanager

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
)

type scriptedScaleInInfoClient struct {
	orchestratorinfo.InfoServiceClient

	mu        sync.Mutex
	responses []*orchestratorinfo.ServiceInfoResponse
	calls     int
}

func (c *scriptedScaleInInfoClient) ServiceInfo(context.Context, *emptypb.Empty, ...grpc.CallOption) (*orchestratorinfo.ServiceInfoResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.calls++
	if len(c.responses) == 0 {
		return nil, errors.New("unexpected ServiceInfo call")
	}
	response := c.responses[0]
	if len(c.responses) > 1 {
		c.responses = c.responses[1:]
	}

	return response, nil
}

type scriptedScaleInSandboxClient struct {
	orchestrator.SandboxServiceClient

	response *orchestrator.SandboxListResponse
	calls    int
}

func (c *scriptedScaleInSandboxClient) List(context.Context, *emptypb.Empty, ...grpc.CallOption) (*orchestrator.SandboxListResponse, error) {
	c.calls++

	return c.response, nil
}

func scaleInInfo(serviceID string, supported, ready bool, running, starts, cleanups, uploads uint64) *orchestratorinfo.ServiceInfoResponse {
	return &orchestratorinfo.ServiceInfoResponse{
		NodeId:                    "node-1",
		ServiceId:                 serviceID,
		ServiceStatus:             orchestratorinfo.ServiceInfoStatus_Draining,
		SafeScaleInSupported:      supported,
		ShutdownReady:             ready,
		MetricSandboxesRunning:    uint32(running),
		SandboxStartsInFlight:     starts,
		LifecycleCleanupsInFlight: cleanups,
		SnapshotUploadsInFlight:   uploads,
	}
}

func testScaleInNode(info orchestratorinfo.InfoServiceClient, sandboxes orchestrator.SandboxServiceClient) *Node {
	node := NewTestNode("node-1", api.NodeStatusReady, 0, 1)
	node.client.Info = info
	node.client.Sandbox = sandboxes

	return node
}

func TestReadWorkerScaleInStateRejectsRestartBetweenInfoAndList(t *testing.T) {
	t.Parallel()

	info := &scriptedScaleInInfoClient{responses: []*orchestratorinfo.ServiceInfoResponse{
		scaleInInfo("service-old", true, true, 0, 0, 0, 0),
		scaleInInfo("service-new", true, false, 0, 0, 0, 0),
	}}
	sandboxes := &scriptedScaleInSandboxClient{response: &orchestrator.SandboxListResponse{}}
	node := testScaleInNode(info, sandboxes)

	_, err := node.ReadWorkerScaleInState(t.Context(), "service-old")

	require.ErrorIs(t, err, ErrScaleInIdentityChanged)
	require.Equal(t, 2, info.calls)
	require.Equal(t, 1, sandboxes.calls)
}

func TestReadWorkerScaleInStateRejectsLegacyProtocolBeforeList(t *testing.T) {
	t.Parallel()

	info := &scriptedScaleInInfoClient{responses: []*orchestratorinfo.ServiceInfoResponse{
		scaleInInfo("service-1", false, false, 0, 0, 0, 0),
	}}
	sandboxes := &scriptedScaleInSandboxClient{response: &orchestrator.SandboxListResponse{}}
	node := testScaleInNode(info, sandboxes)

	_, err := node.ReadWorkerScaleInState(t.Context(), "service-1")

	require.ErrorIs(t, err, ErrScaleInProtocolMissing)
	require.Zero(t, sandboxes.calls)
}

func TestReadWorkerScaleInStateUsesLiveStateInsteadOfCandidateCache(t *testing.T) {
	t.Parallel()

	live := scaleInInfo("service-1", true, false, 1, 2, 3, 4)
	info := &scriptedScaleInInfoClient{responses: []*orchestratorinfo.ServiceInfoResponse{live, live}}
	sandboxes := &scriptedScaleInSandboxClient{response: &orchestrator.SandboxListResponse{
		Sandboxes: []*orchestrator.RunningSandbox{{}},
	}}
	node := testScaleInNode(info, sandboxes)
	node.scaleIn = scaleInObservation{
		NodeID:            "node-1",
		ServiceInstanceID: "stale-service",
		ProtocolSupported: true,
	}

	state, err := node.ReadWorkerScaleInState(t.Context(), "service-1")

	require.NoError(t, err)
	require.Equal(t, uint64(1), state.RunningSandboxes)
	require.Equal(t, uint64(2), state.StartsInFlight)
	require.Equal(t, uint64(3), state.LifecycleCleanupsInFlight)
	require.Equal(t, uint64(4), state.SnapshotUploadsInFlight)
	require.False(t, state.SandboxListEmpty)
	require.False(t, state.ShutdownReady)
}

func TestReadWorkerScaleInStateIgnoresDivergentReplicaCaches(t *testing.T) {
	t.Parallel()

	live := scaleInInfo("service-1", true, false, 1, 0, 0, 0)
	infoA := &scriptedScaleInInfoClient{responses: []*orchestratorinfo.ServiceInfoResponse{live, live}}
	infoB := &scriptedScaleInInfoClient{responses: []*orchestratorinfo.ServiceInfoResponse{live, live}}
	sandboxesA := &scriptedScaleInSandboxClient{response: &orchestrator.SandboxListResponse{
		Sandboxes: []*orchestrator.RunningSandbox{{}},
	}}
	sandboxesB := &scriptedScaleInSandboxClient{response: &orchestrator.SandboxListResponse{
		Sandboxes: []*orchestrator.RunningSandbox{{}},
	}}
	replicaA := testScaleInNode(infoA, sandboxesA)
	replicaB := testScaleInNode(infoB, sandboxesB)
	replicaA.scaleIn = scaleInObservation{NodeID: "node-1", ServiceInstanceID: "service-1", RunningSandboxes: 0, ProtocolSupported: true}
	replicaB.scaleIn = scaleInObservation{NodeID: "node-1", ServiceInstanceID: "service-1", RunningSandboxes: 99, ProtocolSupported: true}

	stateA, err := replicaA.ReadWorkerScaleInState(t.Context(), "service-1")
	require.NoError(t, err)
	stateB, err := replicaB.ReadWorkerScaleInState(t.Context(), "service-1")
	require.NoError(t, err)

	require.Equal(t, uint64(1), stateA.RunningSandboxes)
	require.Equal(t, stateA.RunningSandboxes, stateB.RunningSandboxes)
	require.Equal(t, stateA.SandboxListEmpty, stateB.SandboxListEmpty)
}

func TestReadWorkerScaleInStateRejectsContradictoryShutdownReady(t *testing.T) {
	t.Parallel()

	live := scaleInInfo("service-1", true, true, 1, 0, 0, 0)
	info := &scriptedScaleInInfoClient{responses: []*orchestratorinfo.ServiceInfoResponse{live, live}}
	sandboxes := &scriptedScaleInSandboxClient{response: &orchestrator.SandboxListResponse{}}
	node := testScaleInNode(info, sandboxes)

	_, err := node.ReadWorkerScaleInState(t.Context(), "service-1")

	require.ErrorIs(t, err, ErrScaleInStateInvalid)
}

func TestCancelWorkerScaleInAcceptsHealthyReplacementWithoutMutation(t *testing.T) {
	t.Parallel()

	replacement := scaleInInfo("service-new", true, false, 0, 0, 0, 0)
	replacement.ServiceStatus = orchestratorinfo.ServiceInfoStatus_Healthy
	info := &scriptedScaleInInfoClient{responses: []*orchestratorinfo.ServiceInfoResponse{replacement}}
	sandboxes := &scriptedScaleInSandboxClient{response: &orchestrator.SandboxListResponse{}}
	node := testScaleInNode(info, sandboxes)

	state, err := node.CancelWorkerScaleIn(t.Context(), "service-old")

	require.NoError(t, err)
	require.Equal(t, "service-new", state.ServiceInstanceID)
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Healthy, state.ServiceStatus)
	require.Zero(t, sandboxes.calls, "replacement recovery must not require or infer an empty worker")
}

func TestCancelWorkerScaleInRejectsNonHealthyReplacement(t *testing.T) {
	t.Parallel()

	replacement := scaleInInfo("service-new", true, false, 0, 0, 0, 0)
	replacement.ServiceStatus = orchestratorinfo.ServiceInfoStatus_Unhealthy
	info := &scriptedScaleInInfoClient{responses: []*orchestratorinfo.ServiceInfoResponse{replacement}}
	node := testScaleInNode(info, &scriptedScaleInSandboxClient{response: &orchestrator.SandboxListResponse{}})

	_, err := node.CancelWorkerScaleIn(t.Context(), "service-old")

	require.ErrorIs(t, err, ErrScaleInIdentityChanged)
}
