package nodemanager

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
)

type scriptedScaleInInfoClient struct {
	orchestratorinfo.InfoServiceClient

	mu                      sync.Mutex
	responses               []*orchestratorinfo.ServiceInfoResponse
	calls                   int
	overrideErr             error
	overrideCalls           int
	lastOverrideServiceID   string
	lastOverrideOperationID string
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

func (c *scriptedScaleInInfoClient) ConditionalServiceStatusOverride(_ context.Context, request *orchestratorinfo.ServiceStatusChangeRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.overrideCalls++
	c.lastOverrideServiceID = request.GetExpectedServiceId()
	c.lastOverrideOperationID = request.GetOperationId()

	return &emptypb.Empty{}, c.overrideErr
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
		ScaleInOperationId:        "operation-1",
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

	_, err := node.ReadWorkerScaleInState(t.Context(), "service-old", "operation-1")

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

	_, err := node.ReadWorkerScaleInState(t.Context(), "service-1", "operation-1")

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

	state, err := node.ReadWorkerScaleInState(t.Context(), "service-1", "operation-1")

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

	stateA, err := replicaA.ReadWorkerScaleInState(t.Context(), "service-1", "operation-1")
	require.NoError(t, err)
	stateB, err := replicaB.ReadWorkerScaleInState(t.Context(), "service-1", "operation-1")
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

	_, err := node.ReadWorkerScaleInState(t.Context(), "service-1", "operation-1")

	require.ErrorIs(t, err, ErrScaleInStateInvalid)
}

func TestReadWorkerScaleInStateRejectsCompetingOperation(t *testing.T) {
	t.Parallel()

	live := scaleInInfo("service-1", true, true, 0, 0, 0, 0)
	info := &scriptedScaleInInfoClient{responses: []*orchestratorinfo.ServiceInfoResponse{live, live}}
	node := testScaleInNode(info, &scriptedScaleInSandboxClient{response: &orchestrator.SandboxListResponse{}})

	_, err := node.ReadWorkerScaleInState(t.Context(), "service-1", "operation-2")

	require.ErrorIs(t, err, ErrScaleInIdentityChanged)
}

func TestCancelWorkerScaleInRejectsReplacementIdentity(t *testing.T) {
	t.Parallel()

	info := &scriptedScaleInInfoClient{overrideErr: status.Error(codes.FailedPrecondition, "identity changed")}
	sandboxes := &scriptedScaleInSandboxClient{response: &orchestrator.SandboxListResponse{}}
	node := testScaleInNode(info, sandboxes)

	_, err := node.CancelWorkerScaleIn(t.Context(), "service-old", "operation-1")

	require.ErrorIs(t, err, ErrScaleInIdentityChanged)
	require.Equal(t, 1, info.overrideCalls)
	require.Zero(t, sandboxes.calls, "replacement recovery must not require or infer an empty worker")
}

func TestCancelWorkerScaleInRejectsAlreadyHealthyWithoutOwnedDrain(t *testing.T) {
	t.Parallel()

	info := &scriptedScaleInInfoClient{overrideErr: status.Error(codes.FailedPrecondition, "not owned")}
	node := testScaleInNode(info, &scriptedScaleInSandboxClient{response: &orchestrator.SandboxListResponse{}})

	_, err := node.CancelWorkerScaleIn(t.Context(), "service-1", "operation-1")

	require.ErrorIs(t, err, ErrScaleInIdentityChanged)
	require.Equal(t, 1, info.overrideCalls)
}

func TestCancelWorkerScaleInReplaysOperationOwnerAfterLostResponse(t *testing.T) {
	t.Parallel()

	healthy := scaleInInfo("service-1", true, false, 0, 0, 0, 0)
	healthy.ServiceStatus = orchestratorinfo.ServiceInfoStatus_Healthy
	info := &scriptedScaleInInfoClient{responses: []*orchestratorinfo.ServiceInfoResponse{healthy, healthy}}
	node := testScaleInNode(info, &scriptedScaleInSandboxClient{response: &orchestrator.SandboxListResponse{}})

	_, err := node.CancelWorkerScaleIn(t.Context(), "service-1", "operation-1")

	require.NoError(t, err)
	require.Equal(t, "operation-1", info.lastOverrideOperationID)

	_, err = node.CancelWorkerScaleIn(t.Context(), "service-1", "operation-1")
	require.NoError(t, err)
	require.Equal(t, 2, info.overrideCalls)
}

func TestBeginWorkerScaleInDoesNotMutateLegacyWorker(t *testing.T) {
	t.Parallel()

	live := scaleInInfo("service-1", true, false, 0, 0, 0, 0)
	live.ServiceStatus = orchestratorinfo.ServiceInfoStatus_Healthy
	info := &scriptedScaleInInfoClient{
		responses:   []*orchestratorinfo.ServiceInfoResponse{live},
		overrideErr: status.Error(codes.Unimplemented, "method not implemented"),
	}
	node := testScaleInNode(info, &scriptedScaleInSandboxClient{response: &orchestrator.SandboxListResponse{}})

	_, err := node.BeginWorkerScaleIn(t.Context(), "service-1", "operation-1")

	require.Equal(t, codes.Unimplemented, status.Code(err))
	require.Equal(t, 1, info.overrideCalls)
	require.Equal(t, "service-1", info.lastOverrideServiceID)
	require.Equal(t, "operation-1", info.lastOverrideOperationID)
}

func TestCancelWorkerScaleInRejectsNonHealthyReplacement(t *testing.T) {
	t.Parallel()

	info := &scriptedScaleInInfoClient{}
	node := testScaleInNode(info, &scriptedScaleInSandboxClient{response: &orchestrator.SandboxListResponse{}})

	info.overrideErr = status.Error(codes.FailedPrecondition, "identity changed")
	_, err := node.CancelWorkerScaleIn(t.Context(), "service-old", "operation-1")

	require.ErrorIs(t, err, ErrScaleInIdentityChanged)
}
