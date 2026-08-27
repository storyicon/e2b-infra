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
	desired  int32
	readErr  error
	setErr   error
	metadata ScaleWriteMetadata
	attempts []int32
	sets     []int32
}

func (f *fakeScaleTarget) DesiredCapacity(context.Context, string) (int32, error) {
	return f.desired, f.readErr
}

func (f *fakeScaleTarget) SetDesiredCapacity(_ context.Context, _ string, desired int32) (ScaleWriteMetadata, error) {
	f.attempts = append(f.attempts, desired)
	if f.setErr != nil {
		return ScaleWriteMetadata{}, f.setErr
	}
	f.sets = append(f.sets, desired)
	f.desired = desired

	return f.metadata, nil
}

type fakeAuditSink struct {
	events []ScaleAuditEvent
}

func (f *fakeAuditSink) Record(event ScaleAuditEvent) {
	f.events = append(f.events, event)
}

func newTestReconciler(demand *fakeDemandReader, nodes *fakeNodeCounter, target *fakeScaleTarget) *Reconciler {
	return New(&Config{
		Mode:              ModeLegacyFailureLedger,
		ClusterID:         "cluster-1",
		NodePool:          "orchestrator",
		ASGName:           "workers",
		SlotsPerNode:      20,
		MinNodes:          1,
		MaxNodes:          30,
		BatchIdleDuration: time.Second,
		BatchMaxDuration:  10 * time.Second,
		ReconcileTimeout:  time.Second,
	}, demand, nil, nodes, target)
}

func reconcileStableStartIntent(t *testing.T, r *Reconciler, now time.Time) Result {
	t.Helper()

	initial, err := r.Reconcile(t.Context(), now)
	require.NoError(t, err)
	require.True(t, initial.Aggregating)

	result, err := r.Reconcile(t.Context(), now.Add(time.Second))
	require.NoError(t, err)

	return result
}

func newStartIntentTestReconciler(snapshot *fakeCapacitySnapshotReader, nodes *fakeNodeCounter, target *fakeScaleTarget) *Reconciler {
	return New(&Config{
		Mode:              ModeStartIntentV1,
		ClusterID:         "cluster-1",
		NodePool:          "orchestrator",
		ASGName:           "workers",
		SlotsPerNode:      20,
		MinNodes:          1,
		MaxNodes:          30,
		BatchIdleDuration: time.Second,
		BatchMaxDuration:  10 * time.Second,
		ReconcileTimeout:  time.Second,
	}, nil, snapshot, nodes, target)
}

func TestReconcileStartIntentSizesFiveHundredWorkloadsToTwentyFiveNodes(t *testing.T) {
	t.Parallel()

	snapshot := &fakeCapacitySnapshotReader{snapshot: CapacitySnapshot{WorkloadCount: 500}}
	nodes := &fakeNodeCounter{ready: 1}
	target := &fakeScaleTarget{desired: 1}
	r := newStartIntentTestReconciler(snapshot, nodes, target)

	result := reconcileStableStartIntent(t, r, time.Unix(100, 0))
	require.Equal(t, ModeStartIntentV1, result.Mode)
	require.Equal(t, int64(500), result.WorkloadCount)
	require.Equal(t, int32(1), result.ReadyNodes)
	require.Equal(t, int32(25), result.TargetNodes)
	require.False(t, result.Capped)
	require.Equal(t, []int32{25}, target.sets)
}

func TestReconcileStartIntentScalesAtIdleBoundary(t *testing.T) {
	t.Parallel()

	snapshot := &fakeCapacitySnapshotReader{snapshot: CapacitySnapshot{WorkloadCount: 100}}
	target := &fakeScaleTarget{desired: 1}
	r := newStartIntentTestReconciler(snapshot, &fakeNodeCounter{ready: 1}, target)
	t0 := time.Unix(100, 0)

	first, err := r.Reconcile(t.Context(), t0)
	require.NoError(t, err)
	require.True(t, first.Aggregating)
	require.Empty(t, target.sets)

	snapshot.snapshot.WorkloadCount = 500
	second, err := r.Reconcile(t.Context(), t0.Add(500*time.Millisecond))
	require.NoError(t, err)
	require.True(t, second.Aggregating)
	require.Empty(t, target.sets)

	third, err := r.Reconcile(t.Context(), t0.Add(1500*time.Millisecond))
	require.NoError(t, err)
	require.False(t, third.Aggregating)
	require.Equal(t, int32(25), third.TargetNodes)
	require.Equal(t, []int32{25}, target.sets)
}

func TestReconcileStartIntentScalesAtMaxBoundaryDuringContinuousGrowth(t *testing.T) {
	t.Parallel()

	snapshot := &fakeCapacitySnapshotReader{snapshot: CapacitySnapshot{WorkloadCount: 40}}
	target := &fakeScaleTarget{desired: 1}
	r := newStartIntentTestReconciler(snapshot, &fakeNodeCounter{ready: 1}, target)
	t0 := time.Unix(100, 0)

	for step := 0; step < 10; step++ {
		snapshot.snapshot.WorkloadCount = int64(40 + step*20)
		result, err := r.Reconcile(t.Context(), t0.Add(time.Duration(step)*time.Second))
		require.NoError(t, err)
		require.True(t, result.Aggregating)
	}

	snapshot.snapshot.WorkloadCount = 500
	result, err := r.Reconcile(t.Context(), t0.Add(10*time.Second))
	require.NoError(t, err)
	require.False(t, result.Aggregating)
	require.Equal(t, "max", result.BatchTrigger)
	require.Equal(t, []int32{25}, target.sets)
}

func TestReconcileStartIntentPreservesMaxBoundaryWhenDesiredChangesButDemandIsUnmet(t *testing.T) {
	t.Parallel()

	snapshot := &fakeCapacitySnapshotReader{snapshot: CapacitySnapshot{WorkloadCount: 40}}
	target := &fakeScaleTarget{desired: 1}
	r := newStartIntentTestReconciler(snapshot, &fakeNodeCounter{ready: 1}, target)
	t0 := time.Unix(100, 0)

	for step := 0; step < 10; step++ {
		snapshot.snapshot.WorkloadCount = int64(40 + step*20)
		if step == 9 {
			target.desired = 2
		}
		result, err := r.Reconcile(t.Context(), t0.Add(time.Duration(step)*time.Second))
		require.NoError(t, err)
		require.True(t, result.Aggregating)
	}

	snapshot.snapshot.WorkloadCount = 500
	result, err := r.Reconcile(t.Context(), t0.Add(10*time.Second))
	require.NoError(t, err)
	require.Equal(t, "max", result.BatchTrigger)
	require.Equal(t, []int32{25}, target.sets)
}

func TestReconcileStartIntentStartsNewBatchAfterSuccessfulWrite(t *testing.T) {
	t.Parallel()

	snapshot := &fakeCapacitySnapshotReader{snapshot: CapacitySnapshot{WorkloadCount: 100}}
	target := &fakeScaleTarget{desired: 1}
	r := newStartIntentTestReconciler(snapshot, &fakeNodeCounter{ready: 1}, target)
	t0 := time.Unix(100, 0)

	first := reconcileStableStartIntent(t, r, t0)
	require.Equal(t, "idle", first.BatchTrigger)
	require.Equal(t, []int32{5}, target.sets)

	snapshot.snapshot.WorkloadCount = 500
	newBatch, err := r.Reconcile(t.Context(), t0.Add(2*time.Second))
	require.NoError(t, err)
	require.True(t, newBatch.Aggregating)
	require.Equal(t, []int32{5}, target.sets)

	second, err := r.Reconcile(t.Context(), t0.Add(3*time.Second))
	require.NoError(t, err)
	require.Equal(t, "idle", second.BatchTrigger)
	require.Equal(t, []int32{5, 25}, target.sets)
}

func TestReconcileStartIntentResetsBatchAfterASGWriteFailure(t *testing.T) {
	t.Parallel()

	snapshot := &fakeCapacitySnapshotReader{snapshot: CapacitySnapshot{WorkloadCount: 500}}
	target := &fakeScaleTarget{desired: 1, setErr: errors.New("write failed")}
	r := newStartIntentTestReconciler(snapshot, &fakeNodeCounter{ready: 1}, target)
	t0 := time.Unix(100, 0)

	_, err := r.Reconcile(t.Context(), t0)
	require.NoError(t, err)
	_, err = r.Reconcile(t.Context(), t0.Add(time.Second))
	require.ErrorContains(t, err, "write failed")

	target.setErr = nil
	afterFailure, err := r.Reconcile(t.Context(), t0.Add(2*time.Second))
	require.NoError(t, err)
	require.True(t, afterFailure.Aggregating)
	require.Len(t, target.attempts, 1)
	require.Empty(t, target.sets)

	_, err = r.Reconcile(t.Context(), t0.Add(3*time.Second))
	require.NoError(t, err)
	require.Equal(t, []int32{25, 25}, target.attempts)
	require.Equal(t, []int32{25}, target.sets)
}

func TestReconcileStartIntentAuditsEveryScaleWriteAttempt(t *testing.T) {
	t.Parallel()

	snapshot := &fakeCapacitySnapshotReader{snapshot: CapacitySnapshot{WorkloadCount: 100}}
	target := &fakeScaleTarget{desired: 1, metadata: ScaleWriteMetadata{RequestID: "request-1"}}
	audit := &fakeAuditSink{}
	r := New(&Config{
		Mode:              ModeStartIntentV1,
		ClusterID:         "cluster-1",
		NodePool:          "orchestrator",
		ASGName:           "workers",
		SlotsPerNode:      20,
		MinNodes:          1,
		MaxNodes:          30,
		BatchIdleDuration: time.Second,
		BatchMaxDuration:  10 * time.Second,
		ReconcileTimeout:  time.Second,
	}, nil, snapshot, &fakeNodeCounter{ready: 1}, target, audit)
	t0 := time.Unix(100, 0)

	result := reconcileStableStartIntent(t, r, t0)
	require.True(t, result.Scaled)
	require.Len(t, audit.events, 3)
	startedController, startedWrite, finishedWrite := audit.events[0], audit.events[1], audit.events[2]
	require.Equal(t, AuditEventControllerStarted, startedController.Event)
	require.NotEmpty(t, startedController.ControllerInstanceID)
	require.Equal(t, AuditEventScaleWriteStarted, startedWrite.Event)
	require.Equal(t, uint64(1), startedWrite.ScaleWriteSequence)
	require.Equal(t, int64(100), startedWrite.WorkloadCount)
	require.Equal(t, int32(1), startedWrite.CurrentDesired)
	require.Equal(t, int32(5), startedWrite.Target)
	require.Equal(t, "idle", startedWrite.BatchTrigger)
	require.Equal(t, AuditEventScaleWriteFinished, finishedWrite.Event)
	require.Equal(t, startedWrite.ControllerInstanceID, finishedWrite.ControllerInstanceID)
	require.Equal(t, startedWrite.ScaleWriteSequence, finishedWrite.ScaleWriteSequence)
	require.Equal(t, "success", finishedWrite.Outcome)
	require.Equal(t, "request-1", finishedWrite.AWSRequestID)
	require.Empty(t, finishedWrite.Error)
}

func TestReconcileStartIntentAuditsWriteFailureWithContinuousSequence(t *testing.T) {
	t.Parallel()

	snapshot := &fakeCapacitySnapshotReader{snapshot: CapacitySnapshot{WorkloadCount: 100}}
	target := &fakeScaleTarget{desired: 1, setErr: errors.New("write failed")}
	audit := &fakeAuditSink{}
	r := New(&Config{
		Mode: ModeStartIntentV1, ClusterID: "cluster-1", NodePool: "orchestrator", ASGName: "workers",
		SlotsPerNode: 20, MinNodes: 1, MaxNodes: 30, BatchIdleDuration: time.Second,
		BatchMaxDuration: 10 * time.Second, ReconcileTimeout: time.Second,
	}, nil, snapshot, &fakeNodeCounter{ready: 1}, target, audit)
	t0 := time.Unix(100, 0)

	_, err := r.Reconcile(t.Context(), t0)
	require.NoError(t, err)
	_, err = r.Reconcile(t.Context(), t0.Add(time.Second))
	require.ErrorContains(t, err, "write failed")
	target.setErr = nil
	_, err = r.Reconcile(t.Context(), t0.Add(2*time.Second))
	require.NoError(t, err)
	_, err = r.Reconcile(t.Context(), t0.Add(3*time.Second))
	require.NoError(t, err)

	require.Len(t, audit.events, 5)
	require.Equal(t, []uint64{1, 1, 2, 2}, []uint64{
		audit.events[1].ScaleWriteSequence,
		audit.events[2].ScaleWriteSequence,
		audit.events[3].ScaleWriteSequence,
		audit.events[4].ScaleWriteSequence,
	})
	require.Equal(t, "error", audit.events[2].Outcome)
	require.Equal(t, "write failed", audit.events[2].Error)
	require.Equal(t, "success", audit.events[4].Outcome)
}

func TestNewReconcilerUsesNewAuditIdentityAfterRestart(t *testing.T) {
	t.Parallel()

	firstAudit := &fakeAuditSink{}
	secondAudit := &fakeAuditSink{}
	config := &Config{Mode: ModeStartIntentV1}
	New(config, nil, nil, nil, nil, firstAudit)
	New(config, nil, nil, nil, nil, secondAudit)

	require.Len(t, firstAudit.events, 1)
	require.Len(t, secondAudit.events, 1)
	require.NotEqual(t, firstAudit.events[0].ControllerInstanceID, secondAudit.events[0].ControllerInstanceID)
}

func TestReconcileStartIntentResetsStabilizationAfterSnapshotFailure(t *testing.T) {
	t.Parallel()

	snapshot := &fakeCapacitySnapshotReader{snapshot: CapacitySnapshot{WorkloadCount: 500}}
	target := &fakeScaleTarget{desired: 1}
	r := newStartIntentTestReconciler(snapshot, &fakeNodeCounter{ready: 1}, target)
	t0 := time.Unix(100, 0)

	first, err := r.Reconcile(t.Context(), t0)
	require.NoError(t, err)
	require.True(t, first.Aggregating)

	snapshot.err = errors.New("snapshot unavailable")
	_, err = r.Reconcile(t.Context(), t0.Add(time.Second))
	require.ErrorContains(t, err, "snapshot unavailable")
	snapshot.err = nil

	afterFailure, err := r.Reconcile(t.Context(), t0.Add(2*time.Second))
	require.NoError(t, err)
	require.True(t, afterFailure.Aggregating)
	require.Empty(t, target.sets)

	_, err = r.Reconcile(t.Context(), t0.Add(3*time.Second))
	require.NoError(t, err)
	require.Equal(t, []int32{25}, target.sets)
}

func TestReconcileStartIntentScalesWhenNomadDiagnosticIsUnavailable(t *testing.T) {
	t.Parallel()

	snapshot := &fakeCapacitySnapshotReader{snapshot: CapacitySnapshot{WorkloadCount: 500}}
	nodes := &fakeNodeCounter{err: errors.New("nomad unavailable")}
	target := &fakeScaleTarget{desired: 1}
	r := newStartIntentTestReconciler(snapshot, nodes, target)

	result := reconcileStableStartIntent(t, r, time.Now())
	require.Equal(t, []int32{25}, target.sets)
	require.True(t, result.Scaled)
	require.False(t, result.ReadyNodesObserved)
	require.ErrorContains(t, result.ReadyNodesError, "nomad unavailable")
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

	result := reconcileStableStartIntent(t, r, time.Unix(100, 0))
	require.Equal(t, int32(30), result.TargetNodes)
	require.True(t, result.Capped)
	require.Equal(t, []int32{30}, target.sets)
}

func TestReconcileStartIntentClampsMaximumInt64WorkloadWithoutOverflow(t *testing.T) {
	t.Parallel()

	snapshot := &fakeCapacitySnapshotReader{snapshot: CapacitySnapshot{WorkloadCount: math.MaxInt64}}
	target := &fakeScaleTarget{desired: 1}
	r := newStartIntentTestReconciler(snapshot, &fakeNodeCounter{ready: 1}, target)

	result := reconcileStableStartIntent(t, r, time.Unix(100, 0))
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
		Mode:              ModeStartIntentV1,
		ClusterID:         "cluster-1",
		NodePool:          "orchestrator",
		ASGName:           "workers",
		SlotsPerNode:      20,
		MinNodes:          1,
		MaxNodes:          30,
		BatchIdleDuration: time.Second,
		BatchMaxDuration:  10 * time.Second,
		ReconcileTimeout:  time.Second,
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
		Mode:              ModeStartIntentV1,
		ClusterID:         "cluster-1",
		NodePool:          "orchestrator",
		ASGName:           "workers",
		SlotsPerNode:      20,
		MinNodes:          1,
		MaxNodes:          30,
		BatchIdleDuration: time.Second,
		BatchMaxDuration:  10 * time.Second,
		ReconcileTimeout:  20 * time.Millisecond,
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

func TestReconcileStartIntentRestartsStabilizationAfterRestart(t *testing.T) {
	t.Parallel()

	snapshot := &fakeCapacitySnapshotReader{snapshot: CapacitySnapshot{WorkloadCount: 500}}
	nodes := &fakeNodeCounter{ready: 1}

	firstTarget := &fakeScaleTarget{desired: 1}
	first, err := newStartIntentTestReconciler(snapshot, nodes, firstTarget).Reconcile(t.Context(), time.Unix(100, 0))
	require.NoError(t, err)
	require.True(t, first.Aggregating)
	require.Empty(t, firstTarget.sets)

	secondTarget := &fakeScaleTarget{desired: 1}
	secondReconciler := newStartIntentTestReconciler(snapshot, nodes, secondTarget)
	second, err := secondReconciler.Reconcile(t.Context(), time.Unix(101, 0))
	require.NoError(t, err)
	require.True(t, second.Aggregating)
	require.Empty(t, secondTarget.sets)

	second, err = secondReconciler.Reconcile(t.Context(), time.Unix(102, 0))
	require.NoError(t, err)
	require.False(t, second.Aggregating)
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
		{name: "asg", demand: &fakeDemandReader{}, nodes: &fakeNodeCounter{}, target: &fakeScaleTarget{readErr: errors.New("aws unavailable")}},
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
	target.setErr = targetSetError

	_, err := r.Reconcile(t.Context(), time.Unix(100, 0))
	require.ErrorIs(t, err, targetSetError)
	target.setErr = nil

	result, err := r.Reconcile(t.Context(), time.Unix(101, 0))

	require.NoError(t, err)
	require.True(t, result.Scaled)
	require.Equal(t, []int32{5}, target.sets)
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
