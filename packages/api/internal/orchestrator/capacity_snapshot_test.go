package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/capacity-demand/startintent"
)

type fakeRunningSandboxReader struct {
	items []sandbox.Sandbox
	err   error
}

type fakeWorkloadCounter struct {
	count uint64
	err   error
	calls int
}

func (f *fakeWorkloadCounter) Count(context.Context, string) (uint64, error) {
	f.calls++

	return f.count, f.err
}

type cleanupStartIntentStore struct {
	*fakeStartIntentStore

	removeStarted chan struct{}
	releaseRemove chan struct{}
	deadlines     []time.Time
}

func (s *cleanupStartIntentStore) Remove(ctx context.Context, clusterID, sandboxID, ownerToken string) (bool, error) {
	if s.removeStarted != nil {
		select {
		case s.removeStarted <- struct{}{}:
		default:
		}
	}
	if s.releaseRemove != nil {
		select {
		case <-s.releaseRemove:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	if deadline, ok := ctx.Deadline(); ok {
		s.deadlines = append(s.deadlines, deadline)
	}

	return s.fakeStartIntentStore.Remove(ctx, clusterID, sandboxID, ownerToken)
}

func (f fakeRunningSandboxReader) AllRunningItemsStrict(context.Context) ([]sandbox.Sandbox, error) {
	return f.items, f.err
}

func configureCapacitySnapshotPool(o *Orchestrator) {
	o.capacityPoolVCPU = 1
	o.capacityPoolMemoryMiB = 512
}

func TestCapacitySnapshotUnionsRunningAndActiveIntentIDs(t *testing.T) {
	t.Parallel()

	clusterID := uuid.New()
	otherClusterID := uuid.New()
	intentStore := newFakeStartIntentStore()
	now := time.Now()
	intentStore.records["shared"] = startintent.Record{Intent: startintent.Intent{
		ClusterID: clusterID.String(), SandboxID: "shared", OwnerToken: "owner-shared", VCPU: 1, MemoryMiB: 512, Compatibility: "single-pool-v1",
	}, State: startintent.StateHandoff, CreatedAt: now, ExpiresAt: now.Add(time.Minute)}
	intentStore.records["intent-only"] = startintent.Record{Intent: startintent.Intent{
		ClusterID: clusterID.String(), SandboxID: "intent-only", OwnerToken: "owner-intent", VCPU: 1, MemoryMiB: 512, Compatibility: "single-pool-v1",
	}, State: startintent.StateOutstanding, CreatedAt: now, ExpiresAt: now.Add(time.Minute)}
	intentStore.records["other-intent"] = startintent.Record{Intent: startintent.Intent{
		ClusterID: otherClusterID.String(), SandboxID: "other-intent", OwnerToken: "owner-other", VCPU: 1, MemoryMiB: 512, Compatibility: "single-pool-v1",
	}, State: startintent.StateOutstanding, CreatedAt: now, ExpiresAt: now.Add(time.Minute)}

	o := &Orchestrator{
		startIntentStore: intentStore,
		runningSandboxReader: fakeRunningSandboxReader{items: []sandbox.Sandbox{
			{SandboxID: "shared", ClusterID: clusterID, State: sandbox.StateRunning, VCpu: 1, RamMB: 512},
			{SandboxID: "running-only", ClusterID: clusterID, State: sandbox.StateRunning, VCpu: 1, RamMB: 512},
			{SandboxID: "other-running", ClusterID: otherClusterID, State: sandbox.StateRunning},
		}},
	}
	configureCapacitySnapshotPool(o)

	snapshot, err := o.CapacitySnapshot(t.Context(), clusterID.String())
	require.NoError(t, err)
	require.Equal(t, CapacitySnapshot{
		WorkloadCount:     3,
		RunningCount:      2,
		ActiveIntentCount: 2,
		OverlapCount:      1,
	}, snapshot)
	require.Eventually(t, func() bool {
		intentStore.mu.Lock()
		defer intentStore.mu.Unlock()

		_, exists := intentStore.records["shared"]

		return !exists
	}, time.Second, time.Millisecond, "running-visible handoff must be cleaned")
	intentStore.mu.Lock()
	defer intentStore.mu.Unlock()
	require.Contains(t, intentStore.records, "intent-only")
}

func TestCapacitySnapshotWorkloadV2UsesOnlyLedgerCount(t *testing.T) {
	t.Parallel()

	counter := &fakeWorkloadCounter{count: 10_000}
	o := &Orchestrator{
		capacityDemandMode: cfg.SandboxCapacityDemandModeWorkloadV2,
		workloadCounter:    counter,
		startIntentStore:   nil,
		runningSandboxReader: fakeRunningSandboxReader{
			err: errors.New("legacy reader must not be called"),
		},
	}

	snapshot, err := o.CapacitySnapshot(t.Context(), uuid.NewString())
	require.NoError(t, err)
	require.Equal(t, uint64(10_000), snapshot.WorkloadCount)
	require.Equal(t, 1, counter.calls)
}

func TestCapacitySnapshotWorkloadV2FailsClosedWithoutLegacyFallback(t *testing.T) {
	t.Parallel()

	counter := &fakeWorkloadCounter{err: errors.New("ledger unavailable")}
	o := &Orchestrator{
		capacityDemandMode:   cfg.SandboxCapacityDemandModeWorkloadV2,
		workloadCounter:      counter,
		startIntentStore:     newFakeStartIntentStore(),
		runningSandboxReader: fakeRunningSandboxReader{},
	}

	_, err := o.CapacitySnapshot(t.Context(), uuid.NewString())
	require.ErrorContains(t, err, "ledger unavailable")
	require.Equal(t, 1, counter.calls)
}

func TestCapacitySnapshotShadowKeepsLegacyInputWhenLedgerDiffersOrFails(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		counter *fakeWorkloadCounter
	}{
		{name: "different count", counter: &fakeWorkloadCounter{count: 99}},
		{name: "ledger unavailable", counter: &fakeWorkloadCounter{err: errors.New("ledger unavailable")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			clusterID := uuid.New()
			o := &Orchestrator{
				capacityDemandMode:   cfg.SandboxCapacityDemandModeWorkloadV2Shadow,
				workloadCounter:      tc.counter,
				startIntentStore:     newFakeStartIntentStore(),
				runningSandboxReader: fakeRunningSandboxReader{},
			}
			configureCapacitySnapshotPool(o)

			snapshot, err := o.CapacitySnapshot(t.Context(), clusterID.String())
			require.NoError(t, err)
			require.Zero(t, snapshot.WorkloadCount, "shadow mode must preserve the legacy autoscaler input")
			require.Equal(t, 1, tc.counter.calls)
		})
	}
}

func TestCapacitySnapshotRejectsRunningSandboxForDifferentPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vcpu      int64
		memoryMiB int64
	}{
		{name: "missing resources", vcpu: 0, memoryMiB: 0},
		{name: "vCPU", vcpu: 2, memoryMiB: 512},
		{name: "memory", vcpu: 1, memoryMiB: 1024},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			clusterID := uuid.New()
			o := &Orchestrator{
				startIntentStore: newFakeStartIntentStore(),
				runningSandboxReader: fakeRunningSandboxReader{items: []sandbox.Sandbox{{
					SandboxID: "incompatible-running",
					ClusterID: clusterID,
					State:     sandbox.StateRunning,
					VCpu:      tt.vcpu,
					RamMB:     tt.memoryMiB,
				}}},
			}
			configureCapacitySnapshotPool(o)

			_, err := o.CapacitySnapshot(t.Context(), clusterID.String())

			require.ErrorContains(t, err, "incompatible running sandbox")
		})
	}
}

func TestCapacitySnapshotRejectsIntentForDifferentPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		vcpu          int64
		memoryMiB     int64
		compatibility string
	}{
		{name: "compatibility", vcpu: 1, memoryMiB: 512, compatibility: "single-pool-v1:different"},
		{name: "vCPU", vcpu: 2, memoryMiB: 512, compatibility: "single-pool-v1"},
		{name: "memory", vcpu: 1, memoryMiB: 1024, compatibility: "single-pool-v1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			clusterID := uuid.New()
			intentStore := newFakeStartIntentStore()
			now := time.Now()
			intentStore.records["incompatible"] = startintent.Record{Intent: startintent.Intent{
				ClusterID: clusterID.String(), SandboxID: "incompatible", OwnerToken: "owner", VCPU: tt.vcpu, MemoryMiB: tt.memoryMiB, Compatibility: tt.compatibility,
			}, State: startintent.StateOutstanding, CreatedAt: now, ExpiresAt: now.Add(time.Minute)}
			o := &Orchestrator{
				startIntentStore:     intentStore,
				runningSandboxReader: fakeRunningSandboxReader{},
			}
			configureCapacitySnapshotPool(o)

			_, err := o.CapacitySnapshot(t.Context(), clusterID.String())

			require.ErrorContains(t, err, "incompatible start intent")
		})
	}
}

func TestCapacitySnapshotDoesNotWaitForObservedHandoffCleanup(t *testing.T) {
	t.Parallel()

	clusterID := uuid.New()
	baseStore := newFakeStartIntentStore()
	now := time.Now()
	baseStore.records["shared"] = startintent.Record{Intent: startintent.Intent{
		ClusterID: clusterID.String(), SandboxID: "shared", OwnerToken: "owner", VCPU: 1, MemoryMiB: 512, Compatibility: "single-pool-v1",
	}, State: startintent.StateHandoff, CreatedAt: now, ExpiresAt: now.Add(time.Minute)}
	store := &cleanupStartIntentStore{
		fakeStartIntentStore: baseStore,
		removeStarted:        make(chan struct{}, 1),
		releaseRemove:        make(chan struct{}),
	}
	o := &Orchestrator{
		startIntentStore: store,
		runningSandboxReader: fakeRunningSandboxReader{items: []sandbox.Sandbox{
			{SandboxID: "shared", ClusterID: clusterID, State: sandbox.StateRunning, VCpu: 1, RamMB: 512},
		}},
	}
	configureCapacitySnapshotPool(o)

	type snapshotResult struct {
		snapshot CapacitySnapshot
		err      error
	}
	resultCh := make(chan snapshotResult, 1)
	go func() {
		snapshot, err := o.CapacitySnapshot(t.Context(), clusterID.String())
		resultCh <- snapshotResult{snapshot: snapshot, err: err}
	}()

	select {
	case result := <-resultCh:
		require.NoError(t, result.err)
		require.Equal(t, uint64(1), result.snapshot.WorkloadCount)
	case <-time.After(100 * time.Millisecond):
		close(store.releaseRemove)
		<-resultCh
		t.Fatal("capacity snapshot waited for handoff cleanup")
	}
	close(store.releaseRemove)
	require.Eventually(t, func() bool {
		baseStore.mu.Lock()
		defer baseStore.mu.Unlock()

		_, exists := baseStore.records["shared"]

		return !exists
	}, time.Second, time.Millisecond)
}

func TestObservedHandoffCleanupSharesOneTotalDeadline(t *testing.T) {
	t.Parallel()

	clusterID := uuid.New().String()
	baseStore := newFakeStartIntentStore()
	now := time.Now()
	intents := []startintent.Record{
		{Intent: startintent.Intent{ClusterID: clusterID, SandboxID: "one", OwnerToken: "owner-one", VCPU: 1, MemoryMiB: 512, Compatibility: "single-pool-v1"}, State: startintent.StateHandoff, CreatedAt: now, ExpiresAt: now.Add(time.Minute)},
		{Intent: startintent.Intent{ClusterID: clusterID, SandboxID: "two", OwnerToken: "owner-two", VCPU: 1, MemoryMiB: 512, Compatibility: "single-pool-v1"}, State: startintent.StateHandoff, CreatedAt: now, ExpiresAt: now.Add(time.Minute)},
	}
	for _, intent := range intents {
		baseStore.records[intent.SandboxID] = intent
	}
	store := &cleanupStartIntentStore{fakeStartIntentStore: baseStore}
	o := &Orchestrator{startIntentStore: store}
	configureCapacitySnapshotPool(o)

	o.cleanupObservedHandoffs(t.Context(), intents)

	require.Len(t, store.deadlines, 2)
	require.Equal(t, store.deadlines[0], store.deadlines[1])
}

func TestCapacitySnapshotFailsClosedOnEitherAuthorityFailure(t *testing.T) {
	t.Parallel()

	clusterID := uuid.New().String()
	tests := []struct {
		name       string
		intentErr  error
		runningErr error
	}{
		{name: "intent store", intentErr: errors.New("intent Redis unavailable")},
		{name: "running store", runningErr: errors.New("running Redis unavailable")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			intentStore := newFakeStartIntentStore()
			intentStore.activeErr = tt.intentErr
			o := &Orchestrator{
				startIntentStore:     intentStore,
				runningSandboxReader: fakeRunningSandboxReader{err: tt.runningErr},
			}
			configureCapacitySnapshotPool(o)

			_, err := o.CapacitySnapshot(t.Context(), clusterID)
			require.Error(t, err)
		})
	}
}

func TestCapacitySnapshotRejectsInvalidClusterID(t *testing.T) {
	t.Parallel()

	o := &Orchestrator{startIntentStore: newFakeStartIntentStore(), runningSandboxReader: fakeRunningSandboxReader{}}
	_, err := o.CapacitySnapshot(t.Context(), "not-a-uuid")
	require.Error(t, err)
}

var _ runningSandboxReader = fakeRunningSandboxReader{}
