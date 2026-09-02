//go:build linux

package service

import (
	"context"
	"sync"
	"testing"

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
			info.SetStatus(context.Background(), orchestratorinfo.ServiceInfoStatus_Draining)
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
	info.SetStatus(t.Context(), orchestratorinfo.ServiceInfoStatus_Draining)
	info.SetStatus(t.Context(), orchestratorinfo.ServiceInfoStatus_Unhealthy)

	applied, identityMatched := info.OverrideStatusForService(
		t.Context(),
		orchestratorinfo.ServiceInfoStatus_Healthy,
		"service-1",
	)

	require.True(t, identityMatched)
	require.False(t, applied)
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Unhealthy, info.GetAdmissionState().Status.Status)
}

func TestScaleInCancellationRestoresOwnedDrainingStatus(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{ServiceId: "service-1"}
	info.SetStatus(t.Context(), orchestratorinfo.ServiceInfoStatus_Draining)

	applied, identityMatched := info.OverrideStatusForService(
		t.Context(),
		orchestratorinfo.ServiceInfoStatus_Healthy,
		"service-1",
	)

	require.True(t, identityMatched)
	require.True(t, applied)
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Healthy, info.GetAdmissionState().Status.Status)
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
