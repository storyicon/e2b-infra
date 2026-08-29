package nodemanager

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
)

func TestPlacementMetricsTryReserveIsAtomic(t *testing.T) {
	t.Parallel()

	metrics := newPlacementMetrics()
	metrics.SetCreateConcurrencyLimit(16)

	var ready sync.WaitGroup
	ready.Add(1000)
	start := make(chan struct{})
	var successes atomic.Uint32
	var done sync.WaitGroup
	done.Add(1000)

	for i := range 1000 {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			if metrics.TryReserve(fmt.Sprintf("sandbox-%d", i), SandboxResources{CPUs: 1, MiBMemory: 512}) {
				successes.Add(1)
			}
		}()
	}

	ready.Wait()
	close(start)
	done.Wait()

	require.Equal(t, uint32(16), successes.Load())
	require.Equal(t, uint32(16), metrics.InProgressCount())
}

func TestPlacementMetricsReleaseIsIdempotent(t *testing.T) {
	t.Parallel()

	metrics := newPlacementMetrics()
	metrics.SetCreateConcurrencyLimit(1)
	require.True(t, metrics.TryReserve("sandbox-1", SandboxResources{CPUs: 1, MiBMemory: 512}))
	require.False(t, metrics.TryReserve("sandbox-2", SandboxResources{CPUs: 1, MiBMemory: 512}))

	require.True(t, metrics.Release("sandbox-1"))
	require.False(t, metrics.Release("sandbox-1"), "duplicate cleanup must be a no-op")
	require.Zero(t, metrics.InProgressCount())
	require.True(t, metrics.TryReserve("sandbox-2", SandboxResources{CPUs: 1, MiBMemory: 512}))
	require.False(t, metrics.TryReserve("sandbox-3", SandboxResources{CPUs: 1, MiBMemory: 512}), "duplicate cleanup must not create an extra slot")
}

func TestPlacementMetricsDynamicLimitOnlyBlocksNewReservations(t *testing.T) {
	t.Parallel()

	node := NewTestNode("node-1", api.NodeStatusReady, 0, 4)
	node.UpdateMetricsFromServiceInfoResponse(&orchestratorinfo.ServiceInfoResponse{SandboxCreateConcurrencyLimit: 16})

	for i := range 12 {
		require.True(t, node.PlacementMetrics.TryReserve(fmt.Sprintf("sandbox-%d", i), SandboxResources{}))
	}

	node.UpdateMetricsFromServiceInfoResponse(&orchestratorinfo.ServiceInfoResponse{SandboxCreateConcurrencyLimit: 8})
	require.Equal(t, uint64(8), node.PlacementMetrics.CreateConcurrencyLimit())
	require.Equal(t, uint32(12), node.PlacementMetrics.InProgressCount(), "lowering the limit must not cancel in-flight creates")
	require.False(t, node.PlacementMetrics.TryReserve("sandbox-new", SandboxResources{}))

	for i := range 5 {
		require.True(t, node.PlacementMetrics.Release(fmt.Sprintf("sandbox-%d", i)))
	}
	require.True(t, node.PlacementMetrics.TryReserve("sandbox-new", SandboxResources{}))
}

func TestPlacementMetricsMissingLimitUsesLegacyAdmission(t *testing.T) {
	t.Parallel()

	metrics := newPlacementMetrics()
	require.Zero(t, metrics.CreateConcurrencyLimit())
	require.Equal(t, CreateAdmissionStateLegacyUnbounded, metrics.CreateAdmissionState())

	for i := range 100 {
		require.True(t, metrics.TryReserve(fmt.Sprintf("legacy-%d", i), SandboxResources{}))
	}
	require.Equal(t, uint32(100), metrics.InProgressCount())

	metrics.SetCreateConcurrencyLimit(16)
	require.Equal(t, CreateAdmissionStateBounded, metrics.CreateAdmissionState())
}
