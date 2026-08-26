package controller

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	capacitydemand "github.com/e2b-dev/infra/packages/shared/pkg/capacity-demand"
)

type fakeDemandReader struct {
	summary capacitydemand.Summary
	err     error
	calls   int
}

func (f *fakeDemandReader) Summary(context.Context, string, time.Time) (capacitydemand.Summary, error) {
	f.calls++

	return f.summary, f.err
}

type fakeCapacitySnapshotReader struct {
	snapshot CapacitySnapshot
	err      error
	wait     bool
	deadline time.Time
}

func (f *fakeCapacitySnapshotReader) Snapshot(ctx context.Context, _ string) (CapacitySnapshot, error) {
	f.deadline, _ = ctx.Deadline()
	if f.wait {
		<-ctx.Done()

		return CapacitySnapshot{}, ctx.Err()
	}

	return f.snapshot, f.err
}

type fakeNodeCounter struct {
	ready int32
	err   error
}

func (f *fakeNodeCounter) ReadyCount(context.Context, string) (int32, error) {
	return f.ready, f.err
}

type fakeScaleTarget struct {
	desired int32
	err     error
	sets    []int32
}

func (f *fakeScaleTarget) DesiredCapacity(context.Context, string) (int32, error) {
	return f.desired, f.err
}

func (f *fakeScaleTarget) SetDesiredCapacity(_ context.Context, _ string, desired int32) error {
	f.sets = append(f.sets, desired)
	f.desired = desired

	return nil
}

func newTestReconciler(demand *fakeDemandReader, nodes *fakeNodeCounter, target *fakeScaleTarget) *Reconciler {
	return New(&Config{
		Mode:             ModeLegacyFailureLedger,
		ClusterID:        "cluster-1",
		NodePool:         "orchestrator",
		ASGName:          "workers",
		SlotsPerNode:     20,
		MinNodes:         1,
		MaxNodes:         30,
		ReconcileTimeout: time.Second,
	}, demand, nil, nodes, target)
}

func newStartIntentTestReconciler(snapshot *fakeCapacitySnapshotReader, nodes *fakeNodeCounter, target *fakeScaleTarget) *Reconciler {
	return New(&Config{
		Mode:             ModeStartIntentV1,
		ClusterID:        "cluster-1",
		NodePool:         "orchestrator",
		ASGName:          "workers",
		SlotsPerNode:     20,
		MinNodes:         1,
		MaxNodes:         30,
		ReconcileTimeout: time.Second,
	}, nil, snapshot, nodes, target)
}

func TestReconcileStartIntentSizesFiveHundredWorkloadsToTwentyFiveNodes(t *testing.T) {
	t.Parallel()

	snapshot := &fakeCapacitySnapshotReader{snapshot: CapacitySnapshot{WorkloadCount: 500}}
	nodes := &fakeNodeCounter{ready: 1}
	target := &fakeScaleTarget{desired: 1}
	r := newStartIntentTestReconciler(snapshot, nodes, target)

	result, err := r.Reconcile(t.Context(), time.Unix(100, 0))

	require.NoError(t, err)
	require.Equal(t, ModeStartIntentV1, result.Mode)
	require.Equal(t, int64(500), result.WorkloadCount)
	require.Equal(t, int32(1), result.ReadyNodes)
	require.Equal(t, int32(25), result.TargetNodes)
	require.False(t, result.Capped)
	require.Equal(t, []int32{25}, target.sets)
}

func TestReconcileStartIntentDoesNotRepeatCapacityAlreadyInASGDesired(t *testing.T) {
	t.Parallel()

	snapshot := &fakeCapacitySnapshotReader{snapshot: CapacitySnapshot{WorkloadCount: 500}}
	nodes := &fakeNodeCounter{ready: 1}
	target := &fakeScaleTarget{desired: 25}
	r := newStartIntentTestReconciler(snapshot, nodes, target)

	result, err := r.Reconcile(t.Context(), time.Unix(100, 0))

	require.NoError(t, err)
	require.Equal(t, int32(25), result.TargetNodes)
	require.False(t, result.Scaled)
	require.Empty(t, target.sets)
}

func TestReconcileStartIntentClampsAtMaximumAndReportsCapped(t *testing.T) {
	t.Parallel()

	snapshot := &fakeCapacitySnapshotReader{snapshot: CapacitySnapshot{WorkloadCount: 1_000}}
	nodes := &fakeNodeCounter{ready: 1}
	target := &fakeScaleTarget{desired: 1}
	r := newStartIntentTestReconciler(snapshot, nodes, target)

	result, err := r.Reconcile(t.Context(), time.Unix(100, 0))

	require.NoError(t, err)
	require.Equal(t, int32(30), result.TargetNodes)
	require.True(t, result.Capped)
	require.Equal(t, []int32{30}, target.sets)
}

func TestReconcileStartIntentClampsMaximumInt64WorkloadWithoutOverflow(t *testing.T) {
	t.Parallel()

	snapshot := &fakeCapacitySnapshotReader{snapshot: CapacitySnapshot{WorkloadCount: math.MaxInt64}}
	target := &fakeScaleTarget{desired: 1}
	r := newStartIntentTestReconciler(snapshot, &fakeNodeCounter{ready: 1}, target)

	result, err := r.Reconcile(t.Context(), time.Unix(100, 0))

	require.NoError(t, err)
	require.Equal(t, int32(30), result.TargetNodes)
	require.True(t, result.Capped)
	require.Equal(t, []int32{30}, target.sets)
}

func TestReconcileStartIntentFailsClosedWhenSnapshotReadFails(t *testing.T) {
	t.Parallel()

	legacy := &fakeDemandReader{summary: capacitydemand.Summary{Count: 1_000}}
	snapshot := &fakeCapacitySnapshotReader{err: errors.New("snapshot unavailable")}
	nodes := &fakeNodeCounter{ready: 1}
	target := &fakeScaleTarget{desired: 1}
	r := New(&Config{
		Mode:             ModeStartIntentV1,
		ClusterID:        "cluster-1",
		NodePool:         "orchestrator",
		ASGName:          "workers",
		SlotsPerNode:     20,
		MinNodes:         1,
		MaxNodes:         30,
		ReconcileTimeout: time.Second,
	}, legacy, snapshot, nodes, target)

	result, err := r.Reconcile(t.Context(), time.Unix(100, 0))

	require.ErrorContains(t, err, "read start-intent capacity snapshot")
	require.Equal(t, ModeStartIntentV1, result.Mode)
	require.Zero(t, legacy.calls)
	require.Empty(t, target.sets)
}

func TestReconcileBoundsAllExternalCallsWithOneCycleDeadline(t *testing.T) {
	t.Parallel()

	snapshot := &fakeCapacitySnapshotReader{wait: true}
	target := &fakeScaleTarget{desired: 1}
	r := New(&Config{
		Mode:             ModeStartIntentV1,
		ClusterID:        "cluster-1",
		NodePool:         "orchestrator",
		ASGName:          "workers",
		SlotsPerNode:     20,
		MinNodes:         1,
		MaxNodes:         30,
		ReconcileTimeout: 20 * time.Millisecond,
	}, nil, snapshot, &fakeNodeCounter{ready: 1}, target)
	startedAt := time.Now()

	_, err := r.Reconcile(t.Context(), time.Unix(100, 0))

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.WithinDuration(t, startedAt.Add(20*time.Millisecond), snapshot.deadline, 10*time.Millisecond)
	require.Less(t, time.Since(startedAt), 500*time.Millisecond)
	require.Empty(t, target.sets)
}

func TestReconcileStartIntentRejectsInvalidWorkloadWithoutWritingASG(t *testing.T) {
	t.Parallel()

	snapshot := &fakeCapacitySnapshotReader{snapshot: CapacitySnapshot{WorkloadCount: -1}}
	target := &fakeScaleTarget{desired: 1}
	r := newStartIntentTestReconciler(snapshot, &fakeNodeCounter{ready: 1}, target)

	_, err := r.Reconcile(t.Context(), time.Unix(100, 0))

	require.ErrorContains(t, err, "workload count must be non-negative")
	require.Empty(t, target.sets)
}

func TestReconcileStartIntentRecomputesSameTargetAfterRestart(t *testing.T) {
	t.Parallel()

	snapshot := &fakeCapacitySnapshotReader{snapshot: CapacitySnapshot{WorkloadCount: 500}}
	nodes := &fakeNodeCounter{ready: 1}

	firstTarget := &fakeScaleTarget{desired: 1}
	first, err := newStartIntentTestReconciler(snapshot, nodes, firstTarget).Reconcile(t.Context(), time.Unix(100, 0))
	require.NoError(t, err)

	secondTarget := &fakeScaleTarget{desired: 1}
	second, err := newStartIntentTestReconciler(snapshot, nodes, secondTarget).Reconcile(t.Context(), time.Unix(101, 0))

	require.NoError(t, err)
	require.Equal(t, first.TargetNodes, second.TargetNodes)
	require.Equal(t, []int32{25}, secondTarget.sets)
}

func TestReconcileStartsOneBurstFromCurrentDesiredCapacity(t *testing.T) {
	t.Parallel()

	demand := &fakeDemandReader{summary: capacitydemand.Summary{Count: 480}}
	nodes := &fakeNodeCounter{ready: 1}
	target := &fakeScaleTarget{desired: 1}
	r := newTestReconciler(demand, nodes, target)

	result, err := r.Reconcile(t.Context(), time.Unix(100, 0))

	require.NoError(t, err)
	require.Equal(t, int32(25), result.TargetNodes)
	require.Equal(t, []int32{25}, target.sets)
}

func TestReconcileRetainsFulfilledDemandAcrossBoundedPendingWaves(t *testing.T) {
	t.Parallel()

	demand := &fakeDemandReader{summary: capacitydemand.Summary{Count: 100}}
	nodes := &fakeNodeCounter{ready: 1}
	target := &fakeScaleTarget{desired: 1}
	r := newTestReconciler(demand, nodes, target)

	for index, step := range []struct {
		ready          int32
		pending        int64
		totalFulfilled int64
		target         int32
	}{
		{ready: 1, pending: 100, totalFulfilled: 0, target: 6},
		{ready: 6, pending: 100, totalFulfilled: 100, target: 11},
		{ready: 11, pending: 100, totalFulfilled: 200, target: 16},
		{ready: 16, pending: 100, totalFulfilled: 300, target: 21},
		{ready: 21, pending: 80, totalFulfilled: 400, target: 25},
	} {
		nodes.ready = step.ready
		demand.summary = capacitydemand.Summary{Count: step.pending, TotalFulfilled: step.totalFulfilled}

		result, err := r.Reconcile(t.Context(), time.Unix(100+int64(index), 0))

		require.NoError(t, err)
		require.Equal(t, step.target, result.TargetNodes)
		require.Equal(t, step.pending+step.totalFulfilled, result.BurstDemand)
	}
	require.Equal(t, []int32{6, 11, 16, 21, 25}, target.sets)
}

func TestReconcileCountsDirectSuccessOnlyAfterAdditionalNodesAreReady(t *testing.T) {
	t.Parallel()

	demand := &fakeDemandReader{summary: capacitydemand.Summary{
		Count:              100,
		TotalFulfilled:     531,
		TotalDirectSuccess: 20,
	}}
	nodes := &fakeNodeCounter{ready: 1}
	target := &fakeScaleTarget{desired: 1}
	r := newTestReconciler(demand, nodes, target)

	first, err := r.Reconcile(t.Context(), time.Unix(100, 0))
	require.NoError(t, err)
	require.Equal(t, int64(100), first.BurstDemand)
	require.Equal(t, int32(6), first.TargetNodes)

	// The live failure reached this state: 460 running, 40 pending, but only
	// 389 successes had first entered the pending ledger. Of 71 direct
	// successes, 20 fit on the baseline node and 51 consumed added-node slots.
	nodes.ready = 23
	target.desired = 23
	demand.summary = capacitydemand.Summary{
		Count:              40,
		TotalFulfilled:     920,
		TotalDirectSuccess: 71,
	}

	result, err := r.Reconcile(t.Context(), time.Unix(101, 0))
	require.NoError(t, err)
	require.Equal(t, int64(480), result.BurstDemand)
	require.Equal(t, int32(25), result.TargetNodes)
}

func TestReconcileDoesNotCompoundOneBurstWhileNodesBecomeReady(t *testing.T) {
	t.Parallel()

	demand := &fakeDemandReader{summary: capacitydemand.Summary{Count: 480}}
	nodes := &fakeNodeCounter{ready: 1}
	target := &fakeScaleTarget{desired: 1}
	r := newTestReconciler(demand, nodes, target)

	_, err := r.Reconcile(t.Context(), time.Unix(100, 0))
	require.NoError(t, err)
	nodes.ready = 10

	result, err := r.Reconcile(t.Context(), time.Unix(101, 0))

	require.NoError(t, err)
	require.Equal(t, int32(25), result.TargetNodes)
	require.Equal(t, []int32{25}, target.sets)
}

func TestReconcileRaisesBurstTargetWhenUnmetDemandRises(t *testing.T) {
	t.Parallel()

	demand := &fakeDemandReader{summary: capacitydemand.Summary{Count: 80}}
	nodes := &fakeNodeCounter{ready: 1}
	target := &fakeScaleTarget{desired: 1}
	r := newTestReconciler(demand, nodes, target)

	_, err := r.Reconcile(t.Context(), time.Unix(100, 0))
	require.NoError(t, err)
	demand.summary.Count = 120
	demand.summary.TotalFulfilled = 0

	result, err := r.Reconcile(t.Context(), time.Unix(101, 0))

	require.NoError(t, err)
	require.Equal(t, int32(7), result.TargetNodes)
	require.Equal(t, []int32{5, 7}, target.sets)
}

func TestReconcileRejectsFulfilledCounterRegression(t *testing.T) {
	t.Parallel()

	demand := &fakeDemandReader{summary: capacitydemand.Summary{Count: 80, TotalFulfilled: 100}}
	nodes := &fakeNodeCounter{ready: 1}
	target := &fakeScaleTarget{desired: 1}
	r := newTestReconciler(demand, nodes, target)

	_, err := r.Reconcile(t.Context(), time.Unix(100, 0))
	require.NoError(t, err)
	demand.summary.TotalFulfilled = 99

	_, err = r.Reconcile(t.Context(), time.Unix(101, 0))

	require.ErrorContains(t, err, "total fulfilled regressed")
	require.Equal(t, []int32{5}, target.sets)
}

func TestReconcileResetsBurstAfterPendingDrains(t *testing.T) {
	t.Parallel()

	demand := &fakeDemandReader{summary: capacitydemand.Summary{Count: 80}}
	nodes := &fakeNodeCounter{ready: 1}
	target := &fakeScaleTarget{desired: 1}
	r := newTestReconciler(demand, nodes, target)

	_, err := r.Reconcile(t.Context(), time.Unix(100, 0))
	require.NoError(t, err)
	demand.summary.Count = 0
	_, err = r.Reconcile(t.Context(), time.Unix(101, 0))
	require.NoError(t, err)
	target.desired = 3
	demand.summary.Count = 40
	demand.summary.TotalFulfilled = 0

	result, err := r.Reconcile(t.Context(), time.Unix(102, 0))

	require.NoError(t, err)
	require.Equal(t, int32(5), result.TargetNodes)
	require.Equal(t, []int32{5, 5}, target.sets)
}

func TestReconcileNeverScalesIn(t *testing.T) {
	t.Parallel()

	demand := &fakeDemandReader{}
	nodes := &fakeNodeCounter{ready: 25}
	target := &fakeScaleTarget{desired: 25}
	r := newTestReconciler(demand, nodes, target)

	result, err := r.Reconcile(t.Context(), time.Unix(100, 0))

	require.NoError(t, err)
	require.Equal(t, int32(25), result.TargetNodes)
	require.Empty(t, target.sets)
}

func TestReconcileClampsScaleOutAtMaximum(t *testing.T) {
	t.Parallel()

	demand := &fakeDemandReader{summary: capacitydemand.Summary{Count: 1_000}}
	nodes := &fakeNodeCounter{ready: 1}
	target := &fakeScaleTarget{desired: 1}
	r := newTestReconciler(demand, nodes, target)

	result, err := r.Reconcile(t.Context(), time.Unix(100, 0))

	require.NoError(t, err)
	require.Equal(t, int32(30), result.TargetNodes)
	require.Equal(t, []int32{30}, target.sets)
}

func TestReconcileDoesNotMutateASGWhenAnObservationFails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		demand *fakeDemandReader
		nodes  *fakeNodeCounter
		target *fakeScaleTarget
	}{
		{name: "demand", demand: &fakeDemandReader{err: errors.New("redis unavailable")}, nodes: &fakeNodeCounter{}, target: &fakeScaleTarget{desired: 1}},
		{name: "nomad", demand: &fakeDemandReader{}, nodes: &fakeNodeCounter{err: errors.New("nomad unavailable")}, target: &fakeScaleTarget{desired: 1}},
		{name: "asg", demand: &fakeDemandReader{}, nodes: &fakeNodeCounter{}, target: &fakeScaleTarget{err: errors.New("aws unavailable")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := newTestReconciler(tt.demand, tt.nodes, tt.target)

			_, err := r.Reconcile(t.Context(), time.Unix(100, 0))

			require.Error(t, err)
			require.Empty(t, tt.target.sets)
		})
	}
}

func TestReconcileRetriesScaleOutAfterASGWriteFails(t *testing.T) {
	t.Parallel()

	demand := &fakeDemandReader{summary: capacitydemand.Summary{Count: 80}}
	nodes := &fakeNodeCounter{ready: 1}
	target := &fakeScaleTarget{desired: 1}
	r := newTestReconciler(demand, nodes, target)
	targetSetError := errors.New("write denied")
	targetWithFailure := &failingScaleTarget{fakeScaleTarget: target, setErr: targetSetError}
	r.target = targetWithFailure

	_, err := r.Reconcile(t.Context(), time.Unix(100, 0))
	require.ErrorIs(t, err, targetSetError)
	targetWithFailure.setErr = nil

	result, err := r.Reconcile(t.Context(), time.Unix(101, 0))

	require.NoError(t, err)
	require.True(t, result.Scaled)
	require.Equal(t, []int32{5}, target.sets)
}

type failingScaleTarget struct {
	*fakeScaleTarget

	setErr error
}

func TestParseModeRejectsUnknownValues(t *testing.T) {
	t.Parallel()

	for _, valid := range []string{string(ModeLegacyFailureLedger), string(ModeStartIntentV1)} {
		mode, err := ParseMode(valid)

		require.NoError(t, err)
		require.Equal(t, Mode(valid), mode)
	}

	_, err := ParseMode("automatic")

	require.Error(t, err)
}

func (f *failingScaleTarget) SetDesiredCapacity(ctx context.Context, asgName string, desired int32) error {
	if f.setErr != nil {
		return f.setErr
	}

	return f.fakeScaleTarget.SetDesiredCapacity(ctx, asgName, desired)
}
