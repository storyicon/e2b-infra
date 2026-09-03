//go:build linux

package service

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
)

type mutableCreateLimitProvider struct {
	limit uint64
}

type mutableShutdownActivity struct {
	uploads    uint64
	onSnapshot func()
}

func (p *mutableShutdownActivity) SnapshotUploadsInFlight() uint64 {
	if p.onSnapshot != nil {
		p.onSnapshot()
	}
	return p.uploads
}

func (p *mutableCreateLimitProvider) SandboxCreateConcurrencyLimit() uint64 {
	return p.limit
}

func TestServiceInfoPublishesCurrentSandboxCreateConcurrencyLimit(t *testing.T) {
	t.Parallel()

	provider := &mutableCreateLimitProvider{limit: 16}
	server := NewInfoService(&ServiceInfo{}, sandbox.NewSandboxesMap(), metrics.NewHostMetrics(), provider, &mutableShutdownActivity{})

	response, err := server.ServiceInfo(t.Context(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Equal(t, uint64(16), response.GetSandboxCreateConcurrencyLimit())

	provider.limit = 8
	response, err = server.ServiceInfo(t.Context(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Equal(t, uint64(8), response.GetSandboxCreateConcurrencyLimit(), "ServiceInfo must read the effective runtime limit for each response")
}

func TestShutdownReadyRequiresDrainingAndAllActivityToBeQuiet(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{ServiceId: "service-1"}
	sandboxes := sandbox.NewSandboxesMap()
	activity := &mutableShutdownActivity{}

	state := SnapshotShutdownState(info, sandboxes, activity)
	require.False(t, state.Ready, "a healthy worker must never advertise shutdown readiness")
	require.True(t, state.ActivityQuiet, "local shutdown may finish while an externally fail-closed status is preserved")

	require.True(t, info.AdmitSandboxStart())
	applied, identityMatched := info.OverrideControllerDrain(t.Context(), orchestratorinfo.ServiceInfoStatus_Draining, "service-1", "operation-1")
	require.True(t, applied)
	require.True(t, identityMatched)
	state = SnapshotShutdownState(info, sandboxes, activity)
	require.False(t, state.Ready)
	require.Equal(t, uint64(1), state.SandboxStartsInFlight)
	info.FinishSandboxStart()
	require.True(t, SnapshotShutdownState(info, sandboxes, activity).Ready)

	slot, err := network.NewSlot("test", 1, network.Config{}, network.NoopEgressProxy{})
	require.NoError(t, err)
	sbx := &sandbox.Sandbox{
		LifecycleID: "lifecycle-1",
		Metadata: &sandbox.Metadata{
			Config:  sandbox.NewConfig(sandbox.Config{}),
			Runtime: sandbox.RuntimeMetadata{SandboxID: "sandbox-1"},
		},
		Resources: &sandbox.Resources{Slot: slot},
	}
	sandboxes.MarkRunning(t.Context(), sbx)
	state = SnapshotShutdownState(info, sandboxes, activity)
	require.False(t, state.Ready)
	require.Equal(t, uint64(1), state.LiveSandboxes)
	require.Equal(t, uint64(1), state.LifecycleCleanups)

	require.True(t, sandboxes.MarkStopping(t.Context(), sbx.Runtime.SandboxID, sbx.LifecycleID))
	state = SnapshotShutdownState(info, sandboxes, activity)
	require.False(t, state.Ready, "Delete/Pause must remain blocked while asynchronous cleanup is running")
	require.Zero(t, state.LiveSandboxes)
	require.Equal(t, uint64(1), state.LifecycleCleanups)
	sandboxes.MarkStopped(t.Context(), sbx)
	require.True(t, SnapshotShutdownState(info, sandboxes, activity).Ready)

	activity.uploads = 1
	state = SnapshotShutdownState(info, sandboxes, activity)
	require.False(t, state.Ready)
	require.Equal(t, uint64(1), state.SnapshotUploads)
}

func TestProcessShutdownRevokesExternalShutdownReadiness(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{ServiceId: "service-1"}
	applied, identityMatched := info.OverrideControllerDrain(t.Context(), orchestratorinfo.ServiceInfoStatus_Draining, "service-1", "operation-1")
	require.True(t, applied)
	require.True(t, identityMatched)
	sandboxes := sandbox.NewSandboxesMap()
	activity := &mutableShutdownActivity{}
	require.True(t, SnapshotShutdownState(info, sandboxes, activity).Ready)

	_, draining := info.BeginProcessShutdown(t.Context())
	require.True(t, draining)
	state := SnapshotShutdownState(info, sandboxes, activity)

	require.True(t, state.ActivityQuiet)
	require.False(t, state.Ready)
}

func TestShutdownReadyRechecksOwnershipAfterActivitySnapshot(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{ServiceId: "service-1"}
	applied, identityMatched := info.OverrideControllerDrain(t.Context(), orchestratorinfo.ServiceInfoStatus_Draining, "service-1", "operation-1")
	require.True(t, applied)
	require.True(t, identityMatched)
	activity := &mutableShutdownActivity{onSnapshot: func() {
		info.SetStatus(t.Context(), orchestratorinfo.ServiceInfoStatus_Unhealthy)
	}}

	state := SnapshotShutdownState(info, sandbox.NewSandboxesMap(), activity)

	require.True(t, state.ActivityQuiet)
	require.False(t, state.Ready)
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Unhealthy, state.ServiceStatus.Status)
}

func TestShutdownReadyRejectsOperationReplacementDuringSnapshot(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{ServiceId: "service-1"}
	applied, identityMatched := info.OverrideControllerDrain(t.Context(), orchestratorinfo.ServiceInfoStatus_Draining, "service-1", "operation-1")
	require.True(t, applied)
	require.True(t, identityMatched)
	activity := &mutableShutdownActivity{onSnapshot: func() {
		applied, matched := info.OverrideControllerDrain(t.Context(), orchestratorinfo.ServiceInfoStatus_Healthy, "service-1", "operation-1")
		require.True(t, applied)
		require.True(t, matched)
		applied, matched = info.OverrideControllerDrain(t.Context(), orchestratorinfo.ServiceInfoStatus_Draining, "service-1", "operation-2")
		require.True(t, applied)
		require.True(t, matched)
	}}

	state := SnapshotShutdownState(info, sandbox.NewSandboxesMap(), activity)

	require.False(t, state.Ready)
	require.Equal(t, "operation-2", state.ScaleInOperationID)
}

func TestServiceInfoPublishesOwnedDrainOperation(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{ClientId: "node-1", ServiceId: "service-1"}
	applied, identityMatched := info.OverrideControllerDrain(t.Context(), orchestratorinfo.ServiceInfoStatus_Draining, "service-1", "operation-1")
	require.True(t, applied)
	require.True(t, identityMatched)
	server := NewInfoService(info, sandbox.NewSandboxesMap(), metrics.NewHostMetrics(), &mutableCreateLimitProvider{}, &mutableShutdownActivity{})

	response, err := server.ServiceInfo(t.Context(), &emptypb.Empty{})

	require.NoError(t, err)
	require.True(t, response.GetShutdownReady())
	require.Equal(t, "node-1", response.GetNodeId())
	require.Equal(t, "service-1", response.GetServiceId())
	require.Equal(t, "operation-1", response.GetScaleInOperationId())
}

func TestConditionalServiceStatusOverrideRequiresOperation(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{ServiceId: "service-1"}
	server := &Server{info: info}

	_, err := server.ConditionalServiceStatusOverride(t.Context(), &orchestratorinfo.ServiceStatusChangeRequest{
		ServiceStatus:     orchestratorinfo.ServiceInfoStatus_Draining,
		ExpectedServiceId: "service-1",
	})

	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Healthy, info.GetStatus().Status)
}

func TestServiceStatusOverrideRejectsDrainingToStandby(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{}
	info.SetStatus(t.Context(), orchestratorinfo.ServiceInfoStatus_Draining)
	server := &Server{info: info}

	_, err := server.ServiceStatusOverride(t.Context(), &orchestratorinfo.ServiceStatusChangeRequest{
		ServiceStatus: orchestratorinfo.ServiceInfoStatus_Standby,
	})

	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Draining, info.GetStatus().Status)
}

func TestServiceStatusOverrideRequiresExpectedServiceIdentity(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{ServiceId: "service-current"}
	server := &Server{info: info}

	_, err := server.ServiceStatusOverride(t.Context(), &orchestratorinfo.ServiceStatusChangeRequest{
		ServiceStatus:     orchestratorinfo.ServiceInfoStatus_Draining,
		ExpectedServiceId: "service-old",
	})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Healthy, info.GetStatus().Status)

	_, err = server.ConditionalServiceStatusOverride(t.Context(), &orchestratorinfo.ServiceStatusChangeRequest{
		ServiceStatus:     orchestratorinfo.ServiceInfoStatus_Draining,
		ExpectedServiceId: "service-current",
		OperationId:       "operation-1",
	})
	require.NoError(t, err)
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Draining, info.GetStatus().Status)
}

func TestConditionalServiceStatusOverrideRejectsMissingIdentity(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{ServiceId: "service-current"}
	info.SetStatus(t.Context(), orchestratorinfo.ServiceInfoStatus_Unhealthy)
	server := &Server{info: info}

	_, err := server.ConditionalServiceStatusOverride(t.Context(), &orchestratorinfo.ServiceStatusChangeRequest{
		ServiceStatus: orchestratorinfo.ServiceInfoStatus_Healthy,
	})

	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Unhealthy, info.GetStatus().Status)
}
