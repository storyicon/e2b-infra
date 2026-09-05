//go:build linux

package service

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
)

func TestSandboxStartAdmissionRejectsAfterDrain(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{}
	info.SetStatus(t.Context(), orchestratorinfo.ServiceInfoStatus_Draining)

	admitted := info.AdmitSandboxStart()
	require.False(t, admitted)
	require.Zero(t, info.GetAdmissionState().SandboxStartsInFlight)
}

func TestDrainObservesAlreadyAdmittedSandboxStart(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{}
	admitted := info.AdmitSandboxStart()
	require.True(t, admitted)

	info.SetStatus(t.Context(), orchestratorinfo.ServiceInfoStatus_Draining)
	state := info.GetAdmissionState()
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Draining, state.Status.Status)
	require.Equal(t, uint64(1), state.SandboxStartsInFlight)

	info.FinishSandboxStart()
	require.Zero(t, info.GetAdmissionState().SandboxStartsInFlight)
}

func TestAdmissionAndDrainAreLinearized(t *testing.T) {
	t.Parallel()

	for range 1000 {
		info := &ServiceInfo{}
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)

		var admitted bool
		go func() {
			defer wg.Done()
			<-start
			admitted = info.AdmitSandboxStart()
		}()
		go func() {
			defer wg.Done()
			<-start
			info.SetStatus(t.Context(), orchestratorinfo.ServiceInfoStatus_Draining)
		}()

		close(start)
		wg.Wait()
		state := info.GetAdmissionState()
		require.Equal(t, orchestratorinfo.ServiceInfoStatus_Draining, state.Status.Status)
		if admitted {
			require.Equal(t, uint64(1), state.SandboxStartsInFlight)
			info.FinishSandboxStart()
		} else {
			require.Zero(t, state.SandboxStartsInFlight)
		}
	}
}

func TestScaleInCancellationDoesNotEraseUnhealthyStatus(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{ServiceId: "service-1"}
	applied, identityMatched := info.OverrideControllerDrain(
		t.Context(), orchestratorinfo.ServiceInfoStatus_Draining, "service-1", "operation-1",
	)
	require.True(t, identityMatched)
	require.True(t, applied)
	info.SetStatus(t.Context(), orchestratorinfo.ServiceInfoStatus_Unhealthy)

	applied, identityMatched = info.OverrideControllerDrain(
		t.Context(),
		orchestratorinfo.ServiceInfoStatus_Healthy,
		"service-1",
		"operation-1",
	)

	require.True(t, identityMatched)
	require.False(t, applied)
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Unhealthy, info.GetAdmissionState().Status.Status)
}

func TestScaleInCancellationRestoresOwnedDrainingStatus(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{ServiceId: "service-1"}
	applied, identityMatched := info.OverrideControllerDrain(
		t.Context(), orchestratorinfo.ServiceInfoStatus_Draining, "service-1", "operation-1",
	)
	require.True(t, identityMatched)
	require.True(t, applied)

	applied, identityMatched = info.OverrideControllerDrain(
		t.Context(),
		orchestratorinfo.ServiceInfoStatus_Healthy,
		"service-1",
		"operation-1",
	)

	require.True(t, identityMatched)
	require.True(t, applied)
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Healthy, info.GetAdmissionState().Status.Status)
}

func TestScaleInBeginDoesNotEraseUnhealthyStatus(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{ServiceId: "service-1"}
	info.SetStatus(t.Context(), orchestratorinfo.ServiceInfoStatus_Unhealthy)

	applied, identityMatched := info.OverrideControllerDrain(
		t.Context(),
		orchestratorinfo.ServiceInfoStatus_Draining,
		"service-1",
		"operation-1",
	)

	require.True(t, identityMatched)
	require.False(t, applied)
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Unhealthy, info.GetAdmissionState().Status.Status)
}

func TestScaleInBeginIsIdempotentWhileDraining(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{ServiceId: "service-1"}
	applied, identityMatched := info.OverrideControllerDrain(
		t.Context(), orchestratorinfo.ServiceInfoStatus_Draining, "service-1", "operation-1",
	)
	require.True(t, identityMatched)
	require.True(t, applied)

	applied, identityMatched = info.OverrideControllerDrain(
		t.Context(),
		orchestratorinfo.ServiceInfoStatus_Draining,
		"service-1",
		"operation-1",
	)

	require.True(t, identityMatched)
	require.True(t, applied)
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Draining, info.GetAdmissionState().Status.Status)
	require.Equal(t, "operation-1", info.GetAdmissionState().ControllerDrainOperationID)
}

func TestScaleInBeginRejectsCompetingOperation(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{ServiceId: "service-1"}
	applied, identityMatched := info.OverrideControllerDrain(t.Context(), orchestratorinfo.ServiceInfoStatus_Draining, "service-1", "operation-1")
	require.True(t, identityMatched)
	require.True(t, applied)

	applied, identityMatched = info.OverrideControllerDrain(t.Context(), orchestratorinfo.ServiceInfoStatus_Draining, "service-1", "operation-2")
	require.True(t, identityMatched)
	require.False(t, applied)
	require.Equal(t, "operation-1", info.GetAdmissionState().ControllerDrainOperationID)
}

func TestScaleInCannotClaimLifecycleDrain(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{ServiceId: "service-1"}
	info.SetStatus(t.Context(), orchestratorinfo.ServiceInfoStatus_Draining)

	applied, identityMatched := info.OverrideControllerDrain(
		t.Context(), orchestratorinfo.ServiceInfoStatus_Draining, "service-1", "operation-1",
	)

	require.True(t, identityMatched)
	require.False(t, applied)
}

func TestLifecycleDrainRevokesControllerCancellation(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{ServiceId: "service-1"}
	applied, identityMatched := info.OverrideControllerDrain(
		t.Context(), orchestratorinfo.ServiceInfoStatus_Draining, "service-1", "operation-1",
	)
	require.True(t, identityMatched)
	require.True(t, applied)

	info.SetStatus(t.Context(), orchestratorinfo.ServiceInfoStatus_Draining)
	applied, identityMatched = info.OverrideControllerDrain(
		t.Context(), orchestratorinfo.ServiceInfoStatus_Healthy, "service-1", "operation-1",
	)

	require.True(t, identityMatched)
	require.False(t, applied)
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Draining, info.GetAdmissionState().Status.Status)
}

func TestRejectedLegacyTransitionPreservesControllerCancellation(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{ServiceId: "service-1"}
	applied, identityMatched := info.OverrideControllerDrain(
		t.Context(), orchestratorinfo.ServiceInfoStatus_Draining, "service-1", "operation-1",
	)
	require.True(t, identityMatched)
	require.True(t, applied)

	applied, identityMatched = info.OverrideStatusForService(
		t.Context(), orchestratorinfo.ServiceInfoStatus_Standby, "",
	)
	require.True(t, identityMatched)
	require.False(t, applied)

	applied, identityMatched = info.OverrideControllerDrain(
		t.Context(), orchestratorinfo.ServiceInfoStatus_Healthy, "service-1", "operation-1",
	)
	require.True(t, identityMatched)
	require.True(t, applied)
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Healthy, info.GetAdmissionState().Status.Status)
}

func TestBeginProcessShutdownPreservesUnhealthy(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{}
	info.SetStatus(t.Context(), orchestratorinfo.ServiceInfoStatus_Unhealthy)

	previous, applied := info.BeginProcessShutdown(t.Context())

	require.False(t, applied)
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Unhealthy, previous.Status)
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Unhealthy, info.GetStatus().Status)
}

func TestBeginProcessShutdownRevokesControllerDrainOwnership(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{ServiceId: "service-current"}
	applied, identityMatched := info.OverrideControllerDrain(t.Context(), orchestratorinfo.ServiceInfoStatus_Draining, "service-current", "operation-1")
	require.True(t, applied)
	require.True(t, identityMatched)

	previous, draining := info.BeginProcessShutdown(t.Context())
	require.True(t, draining)
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Draining, previous.Status)

	applied, identityMatched = info.OverrideControllerDrain(t.Context(), orchestratorinfo.ServiceInfoStatus_Healthy, "service-current", "operation-1")
	require.False(t, applied)
	require.True(t, identityMatched)
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Draining, info.GetStatus().Status)
}

func TestControllerDrainAndProcessShutdownAreLinearized(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{ServiceId: "service-current"}
	applied, identityMatched := info.OverrideControllerDrain(t.Context(), orchestratorinfo.ServiceInfoStatus_Draining, "service-current", "operation-1")
	require.True(t, applied)
	require.True(t, identityMatched)

	previous, draining := info.BeginProcessShutdown(t.Context())
	require.True(t, draining)
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Draining, previous.Status)
	require.False(t, info.GetAdmissionState().ControllerDrainOwned)
	require.Empty(t, info.GetAdmissionState().ControllerDrainOperationID)

	applied, identityMatched = info.OverrideControllerDrain(t.Context(), orchestratorinfo.ServiceInfoStatus_Healthy, "service-current", "operation-1")
	require.False(t, applied)
	require.True(t, identityMatched)
}

func TestControllerDrainAndAdmissionAreLinearized(t *testing.T) {
	t.Parallel()

	for range 1000 {
		info := &ServiceInfo{ServiceId: "service-current"}
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)

		var admitted bool
		go func() {
			defer wg.Done()
			<-start
			admitted = info.AdmitSandboxStart()
		}()
		go func() {
			defer wg.Done()
			<-start
			applied, matched := info.OverrideControllerDrain(t.Context(), orchestratorinfo.ServiceInfoStatus_Draining, "service-current", "operation-1")
			assert.True(t, matched)
			assert.True(t, applied)
		}()

		close(start)
		wg.Wait()
		state := info.GetAdmissionState()
		require.Equal(t, orchestratorinfo.ServiceInfoStatus_Draining, state.Status.Status)
		require.Equal(t, "operation-1", state.ControllerDrainOperationID)
		if admitted {
			require.Equal(t, uint64(1), state.SandboxStartsInFlight)
			info.FinishSandboxStart()
		} else {
			require.Zero(t, state.SandboxStartsInFlight)
		}
	}
}

func TestOwnedDrainRejectsLegacyAndInexactCancellation(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{ServiceId: "service-current"}
	applied, identityMatched := info.OverrideControllerDrain(t.Context(), orchestratorinfo.ServiceInfoStatus_Draining, "service-current", "operation-1")
	require.True(t, applied)
	require.True(t, identityMatched)

	applied = info.OverrideStatus(t.Context(), orchestratorinfo.ServiceInfoStatus_Healthy)
	require.False(t, applied)
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Draining, info.GetStatus().Status)
	require.Equal(t, "operation-1", info.GetAdmissionState().ControllerDrainOperationID)

	applied, identityMatched = info.OverrideStatusForService(t.Context(), orchestratorinfo.ServiceInfoStatus_Healthy, "service-current")
	require.False(t, applied)
	require.True(t, identityMatched)
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Draining, info.GetStatus().Status)
	require.Equal(t, "operation-1", info.GetAdmissionState().ControllerDrainOperationID)

	applied, identityMatched = info.OverrideControllerDrain(t.Context(), orchestratorinfo.ServiceInfoStatus_Healthy, "service-current", "operation-2")
	require.False(t, applied)
	require.True(t, identityMatched)
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Draining, info.GetStatus().Status)

	applied, identityMatched = info.OverrideControllerDrain(t.Context(), orchestratorinfo.ServiceInfoStatus_Healthy, "service-current", "operation-1")
	require.True(t, applied)
	require.True(t, identityMatched)
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Healthy, info.GetStatus().Status)
	require.Empty(t, info.GetAdmissionState().ControllerDrainOperationID)
}

func TestControllerDrainCancellationReplayIsIdempotent(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{ServiceId: "service-current"}
	applied, identityMatched := info.OverrideControllerDrain(t.Context(), orchestratorinfo.ServiceInfoStatus_Draining, "service-current", "operation-1")
	require.True(t, applied)
	require.True(t, identityMatched)

	applied, identityMatched = info.OverrideControllerDrain(t.Context(), orchestratorinfo.ServiceInfoStatus_Healthy, "service-current", "operation-1")
	require.True(t, applied)
	require.True(t, identityMatched)

	// Model a lost response by replaying the exact cancellation.
	applied, identityMatched = info.OverrideControllerDrain(t.Context(), orchestratorinfo.ServiceInfoStatus_Healthy, "service-current", "operation-1")
	require.True(t, applied)
	require.True(t, identityMatched)
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Healthy, info.GetStatus().Status)

	// A stale Begin observed before cancellation must not resurrect the completed
	// operation after the cancellation response is lost.
	applied, identityMatched = info.OverrideControllerDrain(t.Context(), orchestratorinfo.ServiceInfoStatus_Draining, "service-current", "operation-1")
	require.False(t, applied)
	require.True(t, identityMatched)
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Healthy, info.GetStatus().Status)

	// A different operation cannot reuse the completed operation's receipt.
	applied, identityMatched = info.OverrideControllerDrain(t.Context(), orchestratorinfo.ServiceInfoStatus_Healthy, "service-current", "operation-2")
	require.False(t, applied)
	require.True(t, identityMatched)

	// A new operation starts normally and replaces the completed receipt.
	applied, identityMatched = info.OverrideControllerDrain(t.Context(), orchestratorinfo.ServiceInfoStatus_Draining, "service-current", "operation-2")
	require.True(t, applied)
	require.True(t, identityMatched)
	require.Equal(t, "operation-2", info.GetAdmissionState().ControllerDrainOperationID)

	applied, identityMatched = info.OverrideControllerDrain(t.Context(), orchestratorinfo.ServiceInfoStatus_Healthy, "service-current", "operation-1")
	require.False(t, applied)
	require.True(t, identityMatched)
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Draining, info.GetStatus().Status)
}

func BenchmarkSandboxStartAdmissionHealthy(b *testing.B) {
	info := &ServiceInfo{}

	b.ReportAllocs()
	for b.Loop() {
		if !info.AdmitSandboxStart() {
			b.Fatal("healthy worker rejected sandbox start")
		}
		info.FinishSandboxStart()
	}
}
