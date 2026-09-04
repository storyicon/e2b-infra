package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type scaleInWorld struct {
	workload                 int64
	cloud                    ScaleInASGSnapshot
	nodes                    []NomadScaleInNode
	workers                  map[string]WorkerScaleInState
	candidates               []ScaleInCandidateObservation
	protectionWrites         []protectionWrite
	desiredWrites            []int32
	protectionErr            error
	protectionErrBeforeApply bool
	desiredErr               error
	snapshotASGErr           error
	listErr                  error
	beginErr                 error
	verifyErr                error
	verifyErrors             map[string]error
	cancelErr                error
	cancelState              *WorkerScaleInState
	markDrainErr             error
	markDrainApplyThenErr    bool
	beginApplyThenErr        bool
	markStageErr             error
	markStageErrors          []error
	restoreErr               error
	restoreApplyThenErr      bool
	cancelApplyThenErr       bool
	markDrainCalls           int
	markStageCalls           int
	restoreCalls             int
	completeCalls            int
	beginCalls               int
	desiredCalls             int
	readyCalls               int
	inventoryCalls           int
	snapshotASGCalls         int
	listCalls                int
	verifyCalls              int
	cancelCalls              int
}

type protectionWrite struct {
	ids       []string
	protected bool
}

func newScaleInWorld(count int, protected bool) *scaleInWorld {
	now := time.Now().Add(-time.Hour)
	w := &scaleInWorld{
		cloud:   ScaleInASGSnapshot{Name: "workers", ARN: "arn:workers", DesiredCapacity: int32(count), MinSize: 1, MaxSize: 1000, NewInstancesProtectedFromScaleIn: true, Instances: make(map[string]ScaleInASGInstance, count)},
		workers: make(map[string]WorkerScaleInState, count),
	}
	for i := range count {
		id := fmt.Sprintf("i-%03d", i)
		nomadID := "nomad-" + id
		serviceID := "service-" + id
		launch := now
		w.cloud.Instances[id] = ScaleInASGInstance{ID: id, HealthStatus: "Healthy", LifecycleState: "InService", ProtectedFromScaleIn: protected, LaunchTime: &launch}
		w.nodes = append(w.nodes, NomadScaleInNode{NomadNodeID: nomadID, NodeID: id, NodePool: "default", Ready: true, Eligible: true, CreateIndex: uint64(i + 1)})
		w.workers[id] = WorkerScaleInState{NodeID: id, ServiceInstanceID: serviceID, ServiceStatus: "Healthy", ScaleInProtocolSupport: true}
	}

	return w
}

func (w *scaleInWorld) armAll() {
	for i := range w.nodes {
		node := &w.nodes[i]
		op := NomadScaleInOperation{OperationID: "op-" + node.NodeID, ServiceInstanceID: "service-" + node.NodeID, StartedAt: time.Now().Add(-time.Minute), Stage: "worker_draining"}
		node.Eligible, node.Draining, node.Operation = false, true, &op
		instance := w.cloud.Instances[node.NodeID]
		instance.ProtectedFromScaleIn = false
		w.cloud.Instances[node.NodeID] = instance
		w.workers[node.NodeID] = readyWorker(node.NodeID, op.ServiceInstanceID, op.OperationID)
	}
}

func (w *scaleInWorld) restoreOrdinary() {
	const nodeID = "i-002"

	instance := w.cloud.Instances[nodeID]
	instance.ProtectedFromScaleIn = true
	w.cloud.Instances[nodeID] = instance
	for i := range w.nodes {
		if w.nodes[i].NodeID == nodeID {
			w.nodes[i].Eligible, w.nodes[i].Draining, w.nodes[i].Operation = true, false, nil
		}
	}
	w.workers[nodeID] = WorkerScaleInState{NodeID: nodeID, ServiceInstanceID: "service-" + nodeID, ServiceStatus: "Healthy", ScaleInProtocolSupport: true}
}

func readyWorker(nodeID, serviceID, operationID string) WorkerScaleInState {
	return WorkerScaleInState{NodeID: nodeID, ServiceInstanceID: serviceID, ServiceStatus: "Draining", ScaleInProtocolSupport: true, ScaleInOperationID: operationID, ShutdownReady: true, SandboxListEmpty: true}
}

func (w *scaleInWorld) Snapshot(context.Context, string) (CapacitySnapshot, error) {
	return CapacitySnapshot{WorkloadCount: w.workload}, nil
}

func (w *scaleInWorld) ReadyCount(context.Context, string) (int32, error) {
	w.readyCalls++

	return int32(len(w.nodes)), nil
}

func (w *scaleInWorld) DesiredCapacity(context.Context, string) (int32, error) {
	w.desiredCalls++

	return w.cloud.DesiredCapacity, nil
}

func (w *scaleInWorld) SetDesiredCapacity(_ context.Context, _ string, desired int32) (ScaleWriteMetadata, error) {
	w.desiredWrites = append(w.desiredWrites, desired)
	w.cloud.DesiredCapacity = desired

	return ScaleWriteMetadata{}, w.desiredErr
}

func (w *scaleInWorld) Inventory(context.Context, string) ([]NomadScaleInNode, error) {
	w.inventoryCalls++

	return slices.Clone(w.nodes), nil
}

func (w *scaleInWorld) MarkDrain(_ context.Context, node NomadScaleInNode, operation NomadScaleInOperation) error {
	w.markDrainCalls++
	if w.markDrainErr != nil && !w.markDrainApplyThenErr {
		return w.markDrainErr
	}
	for i := range w.nodes {
		if w.nodes[i].NomadNodeID == node.NomadNodeID {
			w.nodes[i].Eligible, w.nodes[i].Draining, w.nodes[i].Operation = false, true, &operation

			return w.markDrainErr
		}
	}

	return errors.New("node missing")
}

func (w *scaleInWorld) MarkOperationStage(_ context.Context, node NomadScaleInNode, operation NomadScaleInOperation) error {
	w.markStageCalls++
	if w.markStageCalls <= len(w.markStageErrors) && w.markStageErrors[w.markStageCalls-1] != nil {
		return w.markStageErrors[w.markStageCalls-1]
	}
	if w.markStageErr != nil {
		return w.markStageErr
	}
	for i := range w.nodes {
		if w.nodes[i].NomadNodeID == node.NomadNodeID {
			w.nodes[i].Operation = &operation

			return nil
		}
	}

	return errors.New("node missing")
}

func (w *scaleInWorld) RestoreDrain(_ context.Context, node NomadScaleInNode, operation NomadScaleInOperation) error {
	w.restoreCalls++
	if w.restoreErr != nil && !w.restoreApplyThenErr {
		return w.restoreErr
	}
	for i := range w.nodes {
		if w.nodes[i].NomadNodeID == node.NomadNodeID {
			operation.Stage = "restoring"
			w.nodes[i].Eligible, w.nodes[i].Draining, w.nodes[i].Operation = true, false, &operation

			return w.restoreErr
		}
	}

	return errors.New("node missing")
}

func (w *scaleInWorld) CompleteRestore(_ context.Context, node NomadScaleInNode, operation NomadScaleInOperation) error {
	w.completeCalls++
	for i := range w.nodes {
		if w.nodes[i].NomadNodeID == node.NomadNodeID {
			operation.Stage = "restored"
			w.nodes[i].Eligible, w.nodes[i].Draining, w.nodes[i].Operation = true, false, &operation

			return nil
		}
	}

	return errors.New("node missing")
}

func (w *scaleInWorld) CompleteTermination(_ context.Context, node NomadScaleInNode, operation NomadScaleInOperation) error {
	for i := range w.nodes {
		if w.nodes[i].NomadNodeID == node.NomadNodeID {
			operation.Stage = "complete"
			w.nodes[i].Operation = &operation

			return nil
		}
	}

	return errors.New("node missing")
}

func (w *scaleInWorld) ListScaleInCandidates(_ context.Context, _ string) ([]ScaleInCandidateObservation, error) {
	w.listCalls++
	if w.listErr != nil {
		return nil, w.listErr
	}
	if w.candidates != nil {
		return slices.Clone(w.candidates), nil
	}
	now := time.Now().Add(-time.Millisecond)
	result := make([]ScaleInCandidateObservation, 0, len(w.nodes))
	for _, node := range w.nodes {
		state := w.workers[node.NodeID]
		status := "ready"
		if state.ServiceStatus == "Unhealthy" {
			status = "unhealthy"
		}
		result = append(result, ScaleInCandidateObservation{NodeID: node.NodeID, NomadNodeID: node.NomadNodeID, ServiceInstanceID: state.ServiceInstanceID, ServiceStatus: status, ScaleInProtocolSupport: state.ScaleInProtocolSupport, ObservedAt: now})
	}

	return result, nil
}

func (w *scaleInWorld) BeginWorkerScaleIn(_ context.Context, _, nodeID, serviceID, operationID string) (WorkerScaleInState, error) {
	w.beginCalls++
	if w.beginErr != nil && !w.beginApplyThenErr {
		return WorkerScaleInState{}, w.beginErr
	}
	state := w.workers[nodeID]
	if state.ServiceInstanceID != serviceID {
		return WorkerScaleInState{}, errors.New("identity conflict")
	}
	if state.ScaleInOperationID != "" && state.ScaleInOperationID != operationID {
		return WorkerScaleInState{}, errors.New("operation conflict")
	}
	state = readyWorker(nodeID, serviceID, operationID)
	w.workers[nodeID] = state

	return state, w.beginErr
}

func (w *scaleInWorld) VerifyWorkerScaleIn(_ context.Context, _, nodeID, serviceID, operationID string) (WorkerScaleInState, error) {
	w.verifyCalls++
	if err := w.verifyErrors[nodeID]; err != nil {
		return WorkerScaleInState{}, err
	}
	if w.verifyErr != nil {
		return WorkerScaleInState{}, w.verifyErr
	}
	state := w.workers[nodeID]
	if state.ServiceInstanceID != serviceID || state.ScaleInOperationID != operationID {
		return state, errors.New("ownership conflict")
	}

	return state, nil
}

func (w *scaleInWorld) CancelWorkerScaleIn(_ context.Context, _, nodeID, serviceID, operationID string) (WorkerScaleInState, error) {
	w.cancelCalls++
	if w.cancelErr != nil && !w.cancelApplyThenErr {
		return WorkerScaleInState{}, w.cancelErr
	}
	if w.cancelState != nil {
		return *w.cancelState, nil
	}
	state := w.workers[nodeID]
	if state.ServiceInstanceID == serviceID && state.ServiceStatus == "Healthy" && state.ScaleInOperationID == "" {
		return state, nil
	}
	if state.ServiceInstanceID != serviceID || state.ScaleInOperationID != operationID {
		return state, errors.New("ownership conflict")
	}
	state.ServiceStatus, state.ScaleInOperationID, state.ShutdownReady, state.SandboxListEmpty = "Healthy", "", false, false
	w.workers[nodeID] = state
	if w.cancelApplyThenErr {
		return WorkerScaleInState{}, w.cancelErr
	}

	return state, w.cancelErr
}

func (w *scaleInWorld) SnapshotASG() ScaleInASGSnapshot { return w.cloud }

func (w *scaleInWorld) SetInstanceProtection(_ context.Context, _ string, ids []string, protected bool) error {
	w.protectionWrites = append(w.protectionWrites, protectionWrite{ids: slices.Clone(ids), protected: protected})
	if w.protectionErr != nil && w.protectionErrBeforeApply {
		return w.protectionErr
	}
	for _, id := range ids {
		instance := w.cloud.Instances[id]
		instance.ProtectedFromScaleIn = protected
		w.cloud.Instances[id] = instance
	}

	return w.protectionErr
}

// ScaleInInfrastructure.Snapshot and CapacitySnapshotReader.Snapshot have the
// same method name, so the test world exposes infrastructure through a wrapper.
type worldInfrastructure struct{ world *scaleInWorld }

func (i worldInfrastructure) Snapshot(context.Context, string) (ScaleInASGSnapshot, error) {
	i.world.snapshotASGCalls++

	return i.world.cloud, i.world.snapshotASGErr
}

func (i worldInfrastructure) SetInstanceProtection(ctx context.Context, asg string, ids []string, protected bool) error {
	return i.world.SetInstanceProtection(ctx, asg, ids, protected)
}

func newTestScaleInReconciler(w *scaleInWorld) *Reconciler {
	cfg := &Config{Mode: ModeStartIntentV1, ClusterID: "cluster", NodePool: "default", ASGName: "workers", SlotsPerNode: 1, MinNodes: 1, MaxNodes: 1000, BatchIdleDuration: time.Millisecond, BatchMaxDuration: time.Second, ReconcileTimeout: time.Minute, ScaleInMode: ScaleInModeEnforce, ScaleInHeadroom: 0, ScaleInStableFor: 0, ScaleInMinimumAge: 0, ScaleInTimeout: time.Hour}

	return NewWithScaleIn(cfg, nil, w, w, w, w, w, worldInfrastructure{w})
}

func TestStartIntentSteadyStateReusesOneScaleInObservation(t *testing.T) {
	t.Parallel()

	w := newScaleInWorld(3, true)
	r := newTestScaleInReconciler(w)
	r.config.ScaleInStableFor = time.Hour

	result, err := r.Reconcile(t.Context(), time.Now())
	require.NoError(t, err)
	require.Equal(t, int32(2), result.ScaleInExcess)
	require.Equal(t, 1, w.desiredCalls)
	require.Zero(t, w.readyCalls)
	require.Equal(t, 1, w.inventoryCalls)
	require.Equal(t, 1, w.listCalls)
	require.Equal(t, 1, w.snapshotASGCalls)
	require.Empty(t, w.protectionWrites)
	require.Empty(t, w.desiredWrites)

	_, err = r.Reconcile(t.Context(), time.Now().Add(time.Second))
	require.NoError(t, err)
	require.Equal(t, 2, w.desiredCalls)
	require.Zero(t, w.readyCalls)
	require.Equal(t, 2, w.inventoryCalls)
	require.Equal(t, 2, w.listCalls)
	require.Equal(t, 2, w.snapshotASGCalls, "scale-in observations must not be cached across reconciles")
}

func TestStartIntentDoesNotRetryFailedScaleInObservationInSameReconcile(t *testing.T) {
	t.Parallel()

	w := newScaleInWorld(3, true)
	w.snapshotASGErr = errors.New("throttled")

	result, err := newTestScaleInReconciler(w).Reconcile(t.Context(), time.Now())
	require.NoError(t, err, "scale-in observation must not block the raw scale-out path")
	require.ErrorContains(t, result.ScaleInReadError, "throttled")
	require.Equal(t, 1, w.desiredCalls)
	require.Equal(t, 1, w.readyCalls, "independent readiness diagnostics remain available")
	require.Equal(t, 1, w.inventoryCalls)
	require.Equal(t, 1, w.listCalls)
	require.Equal(t, 1, w.snapshotASGCalls)
	require.Empty(t, w.protectionWrites)
	require.Empty(t, w.desiredWrites)
}

func TestScaleInRepairsProtectionBaselineBeforeAnyDesiredWrite(t *testing.T) {
	t.Parallel()

	w := newScaleInWorld(3, true)
	instance := w.cloud.Instances["i-001"]
	instance.ProtectedFromScaleIn = false
	w.cloud.Instances["i-001"] = instance

	_, err := newTestScaleInReconciler(w).reconcileScaleIn(t.Context(), time.Now(), 0)
	require.NoError(t, err)
	require.Equal(t, []protectionWrite{{ids: []string{"i-001"}, protected: true}}, w.protectionWrites)
	require.Empty(t, w.desiredWrites)
}

func TestScaleInArmedWorkersLowerAbsoluteDesired(t *testing.T) {
	t.Parallel()

	w := newScaleInWorld(3, true)
	w.armAll()
	w.restoreOrdinary()

	result, err := newTestScaleInReconciler(w).reconcileScaleIn(t.Context(), time.Now(), 0)
	require.NoError(t, err)
	require.Equal(t, []int32{1}, w.desiredWrites)
	require.Zero(t, result.ScaleInTerminated, "desired reduction is not a confirmed instance departure")
	for _, write := range w.protectionWrites {
		require.False(t, write.protected)
	}
}

func TestScaleInDesiredResponseLostRestartDoesNotDecrementAgain(t *testing.T) {
	t.Parallel()

	w := newScaleInWorld(3, true)
	w.armAll()
	w.restoreOrdinary()
	w.desiredErr = errors.New("response lost")
	_, err := newTestScaleInReconciler(w).reconcileScaleIn(t.Context(), time.Now(), 0)
	require.ErrorContains(t, err, "response lost")
	require.Equal(t, []int32{1}, w.desiredWrites)

	w.desiredErr = nil
	_, err = newTestScaleInReconciler(w).reconcileScaleIn(t.Context(), time.Now(), 0)
	require.NoError(t, err)
	require.Equal(t, []int32{1}, w.desiredWrites, "memberCount > desired waits for AWS membership convergence")
}

func TestScaleOutCompensationExcludesCommittedScaleIn(t *testing.T) {
	t.Parallel()

	w := newScaleInWorld(3, true)
	w.armAll()
	w.restoreOrdinary()
	r := newTestScaleInReconciler(w)

	w.cloud.DesiredCapacity = 1
	required, err := r.scaleOutRequiredForEnforce(t.Context(), time.Now(), 0)
	require.NoError(t, err)
	require.Zero(t, required, "a committed 3-to-1 scale-in must not launch replacements")

	w.cloud.DesiredCapacity = 2
	required, err = r.scaleOutRequiredForEnforce(t.Context(), time.Now(), 2)
	require.NoError(t, err)
	require.Equal(t, int64(3), required, "demand recovery compensates only the drain not covered by the committed deficit")
}

func TestScaleInOverlappingControllersReplayOneAbsoluteTarget(t *testing.T) {
	t.Parallel()

	var writes []int32
	for range 2 {
		// Both reconcilers start from the same stale desired=3 membership view.
		// Each writes the same absolute target instead of applying two relative
		// decrements to shared capacity.
		w := newScaleInWorld(3, true)
		w.armAll()
		w.restoreOrdinary()
		_, err := newTestScaleInReconciler(w).reconcileScaleIn(t.Context(), time.Now(), 0)
		require.NoError(t, err)
		writes = append(writes, w.desiredWrites...)
	}
	require.Equal(t, []int32{1, 1}, writes)
}

func TestScaleInFreshWorkloadBlocksLateStaleReduction(t *testing.T) {
	t.Parallel()

	w := newScaleInWorld(3, true)
	w.armAll()
	w.restoreOrdinary()
	w.workload = 3

	_, err := newTestScaleInReconciler(w).reconcileScaleIn(t.Context(), time.Now(), 0)

	require.NoError(t, err)
	require.Empty(t, w.desiredWrites, "the fresh workload snapshot must override the stale reconcile input")
}

func TestScaleInDesiredIncreaseResponseLostIsNotRepeated(t *testing.T) {
	t.Parallel()

	w := newScaleInWorld(3, true)
	w.armAll()
	w.cloud.DesiredCapacity = 2
	w.workload = 3
	w.desiredErr = errors.New("increase response lost")

	_, err := newTestScaleInReconciler(w).reconcileScaleIn(t.Context(), time.Now(), 3)
	require.ErrorContains(t, err, "response lost")
	require.Equal(t, []int32{3}, w.desiredWrites)

	w.desiredErr = nil
	_, err = newTestScaleInReconciler(w).reconcileScaleIn(t.Context(), time.Now(), 3)
	require.NoError(t, err)
	require.Equal(t, []int32{3}, w.desiredWrites)
}

func TestScaleInDemandRiseRaisesDesiredBeforeProtectionOrAdmission(t *testing.T) {
	t.Parallel()

	w := newScaleInWorld(3, true)
	w.armAll()
	w.cloud.DesiredCapacity = 2
	w.workload = 3

	_, err := newTestScaleInReconciler(w).reconcileScaleIn(t.Context(), time.Now(), 3)
	require.NoError(t, err)
	require.Equal(t, []int32{3}, w.desiredWrites)
	require.Empty(t, w.protectionWrites)
	for _, state := range w.workers {
		require.Equal(t, "Draining", state.ServiceStatus)
	}
}

func TestScaleInProtectionResponseLostRecoversFromFreshState(t *testing.T) {
	t.Parallel()

	w := newScaleInWorld(2, true)
	op := NomadScaleInOperation{OperationID: "op", ServiceInstanceID: "service-i-001", StartedAt: time.Now(), Stage: "worker_draining"}
	w.nodes[1].Eligible, w.nodes[1].Draining, w.nodes[1].Operation = false, true, &op
	w.workers["i-001"] = readyWorker("i-001", op.ServiceInstanceID, op.OperationID)
	w.protectionErr = errors.New("response lost")

	_, err := newTestScaleInReconciler(w).reconcileScaleIn(t.Context(), time.Now(), 0)
	require.ErrorContains(t, err, "response lost")
	require.Empty(t, w.desiredWrites)

	w.protectionErr = nil
	_, err = newTestScaleInReconciler(w).reconcileScaleIn(t.Context(), time.Now(), 0)
	require.NoError(t, err)
	require.Equal(t, []int32{1}, w.desiredWrites)
}

func TestScaleInProtectionResourceContentionDoesNotGuessOutcome(t *testing.T) {
	t.Parallel()

	w := newScaleInWorld(2, true)
	op := NomadScaleInOperation{OperationID: "op", ServiceInstanceID: "service-i-001", StartedAt: time.Now(), Stage: "worker_draining"}
	w.nodes[1].Eligible, w.nodes[1].Draining, w.nodes[1].Operation = false, true, &op
	w.workers["i-001"] = readyWorker("i-001", op.ServiceInstanceID, op.OperationID)
	w.protectionErr = errors.New("ResourceContention")
	w.protectionErrBeforeApply = true
	r := newTestScaleInReconciler(w)

	_, err := r.reconcileScaleIn(t.Context(), time.Now(), 0)
	require.ErrorContains(t, err, "ResourceContention")
	require.True(t, w.cloud.Instances["i-001"].ProtectedFromScaleIn)
	require.Empty(t, w.desiredWrites)

	w.protectionErr = nil
	_, err = r.reconcileScaleIn(t.Context(), time.Now(), 0)
	require.NoError(t, err)
	require.False(t, w.cloud.Instances["i-001"].ProtectedFromScaleIn)
	require.Empty(t, w.desiredWrites, "arming requires a later fresh snapshot before desired reduction")
}

func TestScaleInRestoreProtectionResponseLostUsesFreshState(t *testing.T) {
	t.Parallel()

	w := newScaleInWorld(1, true)
	w.armAll()
	w.workload = 1
	w.protectionErr = errors.New("restore protection response lost")
	r := newTestScaleInReconciler(w)

	_, err := r.reconcileScaleIn(t.Context(), time.Now(), 1)
	require.ErrorContains(t, err, "response lost")
	require.True(t, w.cloud.Instances["i-000"].ProtectedFromScaleIn)

	w.protectionErr = nil
	_, err = r.reconcileScaleIn(t.Context(), time.Now(), 1)
	require.NoError(t, err)
	require.Len(t, w.protectionWrites, 1, "fresh protected state must prevent a duplicate protection write")
	require.Equal(t, "restoring", w.nodes[0].Operation.Stage)
}

func replacementWorld(protected bool, status string, observable bool) *scaleInWorld {
	w := newScaleInWorld(1, protected)
	op := NomadScaleInOperation{OperationID: "old-op", ServiceInstanceID: "old-service", StartedAt: time.Now(), Stage: "worker_draining"}
	w.nodes[0].Ready, w.nodes[0].Eligible, w.nodes[0].Draining, w.nodes[0].Operation = false, false, true, &op
	w.nodes = append(w.nodes, NomadScaleInNode{NomadNodeID: "nomad-new", NodeID: "i-000", NodePool: "default", Ready: true, Eligible: true})
	w.workers["i-000"] = WorkerScaleInState{NodeID: "i-000", ServiceInstanceID: "new-service", ServiceStatus: "Healthy", ScaleInProtocolSupport: true}
	if observable {
		w.candidates = []ScaleInCandidateObservation{{NodeID: "i-000", NomadNodeID: "nomad-new", ServiceInstanceID: "new-service", ServiceStatus: status, ScaleInProtocolSupport: true, ObservedAt: time.Now()}}
	} else {
		w.candidates = []ScaleInCandidateObservation{}
	}

	return w
}

func TestScaleInProtectedConfirmedReplacementOnlyCleansOldNomadOperation(t *testing.T) {
	t.Parallel()

	w := replacementWorld(true, "ready", true)

	r := newTestScaleInReconciler(w)
	_, err := r.reconcileScaleIn(t.Context(), time.Now(), 1)
	require.NoError(t, err)
	require.Equal(t, "restoring", w.nodes[0].Operation.Stage)
	result, err := r.reconcileScaleIn(t.Context(), time.Now(), 1)

	require.NoError(t, err)
	require.Equal(t, int32(1), result.ScaleInCancelled)
	require.Equal(t, "restored", w.nodes[0].Operation.Stage)
	require.Zero(t, w.verifyCalls)
	require.Zero(t, w.cancelCalls, "the replacement Worker must not receive an old-operation Cancel")
	require.Empty(t, w.protectionWrites)
}

func TestScaleInWorkerRestartOnSameNomadNodeOnlyCleansOldNomadOperation(t *testing.T) {
	t.Parallel()

	w := newScaleInWorld(1, true)
	op := NomadScaleInOperation{OperationID: "old-op", ServiceInstanceID: "old-service", StartedAt: time.Now(), Stage: "worker_draining"}
	w.nodes[0].Eligible, w.nodes[0].Draining, w.nodes[0].Operation = false, true, &op
	w.workers["i-000"] = WorkerScaleInState{NodeID: "i-000", ServiceInstanceID: "new-service", ServiceStatus: "Healthy", ScaleInProtocolSupport: true}
	w.candidates = []ScaleInCandidateObservation{{NodeID: "i-000", NomadNodeID: w.nodes[0].NomadNodeID, ServiceInstanceID: "new-service", ServiceStatus: "ready", ScaleInProtocolSupport: true, ObservedAt: time.Now()}}
	r := newTestScaleInReconciler(w)

	_, err := r.reconcileScaleIn(t.Context(), time.Now(), 1)
	require.NoError(t, err)
	require.Equal(t, "restoring", w.nodes[0].Operation.Stage)
	result, err := r.reconcileScaleIn(t.Context(), time.Now(), 1)

	require.NoError(t, err)
	require.Equal(t, int32(1), result.ScaleInCancelled)
	require.Equal(t, "restored", w.nodes[0].Operation.Stage)
	require.Zero(t, w.verifyCalls)
	require.Zero(t, w.cancelCalls, "the restarted Worker must not receive an old-operation Cancel")
}

func TestScaleInReplacementIsNotCleanedWithoutEverySafetyProof(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name        string
		protected   bool
		status      string
		observable  bool
		wantProtect bool
		wantError   bool
	}{
		{name: "unprotected", status: "ready", observable: true, wantProtect: true, wantError: true},
		{name: "unknown", protected: true, observable: false, wantError: true},
		{name: "not healthy", protected: true, status: "unhealthy", observable: true, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			w := replacementWorld(test.protected, test.status, test.observable)
			_, err := newTestScaleInReconciler(w).reconcileScaleIn(t.Context(), time.Now(), 1)

			require.Equal(t, "worker_draining", w.nodes[0].Operation.Stage)
			if test.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if test.wantProtect {
				require.Equal(t, []protectionWrite{{ids: []string{"i-000"}, protected: true}}, w.protectionWrites)
			} else {
				require.Empty(t, w.protectionWrites)
			}
		})
	}
}

func TestScaleInArmsFiftyReadyOperationsWithOneProtectionWrite(t *testing.T) {
	t.Parallel()

	w := newScaleInWorld(51, true)
	for i := range 50 {
		node := &w.nodes[i]
		op := NomadScaleInOperation{OperationID: "op-" + node.NodeID, ServiceInstanceID: "service-" + node.NodeID, StartedAt: time.Now(), Stage: "worker_draining"}
		node.Eligible, node.Draining, node.Operation = false, true, &op
		w.workers[node.NodeID] = readyWorker(node.NodeID, op.ServiceInstanceID, op.OperationID)
	}

	_, err := newTestScaleInReconciler(w).reconcileScaleIn(t.Context(), time.Now(), 0)

	require.NoError(t, err)
	require.Len(t, w.protectionWrites, 1)
	require.False(t, w.protectionWrites[0].protected)
	require.Len(t, w.protectionWrites[0].ids, 50)
	require.Empty(t, w.desiredWrites, "arming must be confirmed by a later fresh snapshot")
}

func TestScaleInOffDoesNotTouchDependencies(t *testing.T) {
	t.Parallel()

	w := newScaleInWorld(2, true)
	r := newTestScaleInReconciler(w)
	r.config.ScaleInMode = ScaleInModeOff
	_, err := r.reconcileScaleIn(t.Context(), time.Now(), 0)
	require.NoError(t, err)
	require.Zero(t, w.listCalls)
	require.Empty(t, w.protectionWrites)
	require.Empty(t, w.desiredWrites)
}

func TestObserveMixedWorkerVersionsNeverMutatesCapacity(t *testing.T) {
	t.Parallel()

	w := newScaleInWorld(2, true)
	w.candidates = []ScaleInCandidateObservation{
		{NodeID: "i-000", NomadNodeID: "nomad-i-000", ServiceInstanceID: "service-i-000", ServiceStatus: "ready", ScaleInProtocolSupport: true, ObservedAt: time.Now()},
		{NodeID: "i-001", NomadNodeID: "nomad-i-001", ServiceInstanceID: "service-i-001", ServiceStatus: "ready", ScaleInProtocolSupport: false, ObservedAt: time.Now()},
	}
	r := newTestScaleInReconciler(w)
	r.config.ScaleInMode = ScaleInModeObserve
	result, err := r.reconcileScaleIn(t.Context(), time.Now(), 0)
	require.NoError(t, err)
	require.Equal(t, int32(2), result.ScaleInAccepting)
	require.Empty(t, w.protectionWrites)
	require.Empty(t, w.desiredWrites)
	require.Zero(t, w.markDrainCalls)
}

func TestCandidateObservationFreshnessFailsClosed(t *testing.T) {
	t.Parallel()

	now := time.Unix(100, 0)
	launch := now.Add(-time.Hour)
	nomad := []NomadScaleInNode{{NomadNodeID: "nomad", NodeID: "i", Ready: true, Eligible: true}}
	cloud := ScaleInASGSnapshot{Instances: map[string]ScaleInASGInstance{"i": {ID: "i", HealthStatus: "Healthy", LifecycleState: "InService", LaunchTime: &launch}}}
	for _, tc := range []struct {
		name     string
		observed time.Time
		ready    bool
	}{
		{"fresh", now.Add(-scaleInCandidateMaxAge), true}, {"expired", now.Add(-scaleInCandidateMaxAge - time.Nanosecond), false}, {"future", now.Add(time.Nanosecond), false}, {"missing", time.Time{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			nodes := mergeScaleInNodes(now, nomad, []ScaleInCandidateObservation{{NodeID: "i", NomadNodeID: "nomad", ServiceInstanceID: "service", ServiceStatus: "ready", ScaleInProtocolSupport: true, ObservedAt: tc.observed}}, cloud)
			require.Equal(t, tc.ready, nodes[0].Healthy)
			require.Equal(t, tc.ready, nodes[0].ScaleInProtocolSupport)
		})
	}
}

func TestCurrentASGInstanceWithoutNomadConsumesDisruptionBudget(t *testing.T) {
	t.Parallel()

	now := time.Now()
	launch := now.Add(-time.Hour)
	cloud := ScaleInASGSnapshot{Instances: map[string]ScaleInASGInstance{"i-known": {ID: "i-known", HealthStatus: "Healthy", LifecycleState: "InService", LaunchTime: &launch}, "i-unknown": {ID: "i-unknown", HealthStatus: "Healthy", LifecycleState: "InService", LaunchTime: &launch}}}
	nodes := mergeScaleInNodes(now, []NomadScaleInNode{{NomadNodeID: "nomad", NodeID: "i-known", Ready: true, Eligible: true}}, []ScaleInCandidateObservation{{NodeID: "i-known", NomadNodeID: "nomad", ServiceInstanceID: "service", ServiceStatus: "ready", ScaleInProtocolSupport: true, ObservedAt: now}}, cloud)
	plan, err := BuildScaleInPlan(0, 1, 0, 0, nodes)
	require.NoError(t, err)
	require.Equal(t, int32(1), plan.DisruptionUsed)
}

func TestDepartedNomadHistoryCompletesWithoutCancellationChurn(t *testing.T) {
	t.Parallel()

	w := newScaleInWorld(2, true)
	op := NomadScaleInOperation{OperationID: "departed", ServiceInstanceID: "service-gone", StartedAt: time.Now(), Stage: "worker_draining"}
	w.nodes = append(w.nodes, NomadScaleInNode{NomadNodeID: "nomad-gone", NodeID: "i-gone", NodePool: "default", Draining: true, Operation: &op})
	r := newTestScaleInReconciler(w)
	result, err := r.reconcileScaleIn(t.Context(), time.Now(), 2)
	require.NoError(t, err)
	require.Equal(t, int32(1), result.ScaleInTerminated)
	require.Zero(t, w.cancelCalls)
	now := time.Now()
	candidates := []ScaleInCandidateObservation{
		{NodeID: "i-000", NomadNodeID: "nomad-i-000", ServiceInstanceID: "service-i-000", ServiceStatus: "ready", ScaleInProtocolSupport: true, ObservedAt: now},
		{NodeID: "i-001", NomadNodeID: "nomad-i-001", ServiceInstanceID: "service-i-001", ServiceStatus: "ready", ScaleInProtocolSupport: true, ObservedAt: now},
	}
	plan, err := BuildScaleInPlan(0, 1, 0, 0, mergeScaleInNodes(now, w.nodes, candidates, w.cloud))
	require.NoError(t, err)
	require.Zero(t, plan.DisruptionUsed, "departed Nomad history is absent from current ASG capacity")
}

func TestEnforceScaleOutContinuesWhenScaleInObservationFails(t *testing.T) {
	t.Parallel()

	w := newScaleInWorld(1, true)
	w.workload = 2
	w.listErr = errors.New("scale-in observation unavailable")
	cfg := &Config{Mode: ModeStartIntentV1, ClusterID: "cluster", NodePool: "default", ASGName: "workers", SlotsPerNode: 1, MinNodes: 1, MaxNodes: 10, BatchIdleDuration: time.Millisecond, BatchMaxDuration: time.Second, ReconcileTimeout: time.Second, ScaleInMode: ScaleInModeEnforce, ScaleInTimeout: time.Hour}
	r := NewWithScaleIn(cfg, nil, w, w, w, w, w, worldInfrastructure{w})
	now := time.Now()
	_, err := r.Reconcile(t.Context(), now)
	require.NoError(t, err)
	result, err := r.Reconcile(t.Context(), now.Add(2*time.Millisecond))
	require.NoError(t, err)
	require.Equal(t, []int32{2}, w.desiredWrites)
	require.ErrorContains(t, result.ScaleInReadError, "observation unavailable")
}

func TestNomadMarkedRecoveryAndRestoringUseExactStateActions(t *testing.T) {
	t.Parallel()

	w := newScaleInWorld(2, true)
	op := NomadScaleInOperation{OperationID: "op", ServiceInstanceID: "service-i-000", StartedAt: time.Now(), Stage: "nomad_marked"}
	w.nodes[0].Eligible, w.nodes[0].Draining, w.nodes[0].Operation = false, true, &op
	r := newTestScaleInReconciler(w)
	_, err := r.reconcileScaleIn(t.Context(), time.Now(), 0)
	require.NoError(t, err)
	require.Equal(t, "worker_draining", w.nodes[0].Operation.Stage)
	require.Equal(t, 1, w.beginCalls)
	require.Zero(t, w.cancelCalls)

	w.nodes[0].Operation.Stage = "restoring"
	w.workers["i-000"] = readyWorker("i-000", op.ServiceInstanceID, op.OperationID)
	_, err = r.reconcileScaleIn(t.Context(), time.Now(), 2)
	require.NoError(t, err)
	require.Equal(t, "restored", w.nodes[0].Operation.Stage)
	require.Equal(t, 1, w.beginCalls)
	require.Equal(t, 1, w.cancelCalls)
}

func TestNomadMarkedBeginFailureKeepsIsolationWhenOwnershipCannotBeVerified(t *testing.T) {
	t.Parallel()

	w := newScaleInWorld(2, true)
	w.beginErr = errors.New("begin failed")
	op := NomadScaleInOperation{OperationID: "op", ServiceInstanceID: "service-i-000", StartedAt: time.Now(), Stage: "nomad_marked"}
	w.nodes[0].Eligible, w.nodes[0].Draining, w.nodes[0].Operation = false, true, &op
	_, err := newTestScaleInReconciler(w).reconcileScaleIn(t.Context(), time.Now(), 0)
	require.ErrorContains(t, err, "begin failed")
	require.Equal(t, "nomad_marked", w.nodes[0].Operation.Stage)
	require.Zero(t, w.cancelCalls)
}

func TestNomadMarkResponseLostStopsBatchAndRecoversFromInventory(t *testing.T) {
	t.Parallel()

	w := newScaleInWorld(3, true)
	w.markDrainErr = errors.New("mark response lost")
	w.markDrainApplyThenErr = true
	r := newTestScaleInReconciler(w)
	now := time.Now()
	r.scaleIn.stabilizer.firstObservedAt = now.Add(-time.Second)

	_, err := r.reconcileScaleIn(t.Context(), now, 0)
	require.ErrorContains(t, err, "response lost")
	require.Equal(t, 1, w.markDrainCalls, "an ambiguous write must stop the batch")
	active := slices.IndexFunc(w.nodes, func(node NomadScaleInNode) bool { return node.Operation != nil })
	require.NotEqual(t, -1, active)
	require.Equal(t, "nomad_marked", w.nodes[active].Operation.Stage)

	w.markDrainErr = nil
	w.markDrainApplyThenErr = false
	_, err = r.reconcileScaleIn(t.Context(), now, 0)
	require.NoError(t, err)
	require.Equal(t, "worker_draining", w.nodes[active].Operation.Stage)
	require.GreaterOrEqual(t, w.beginCalls, 1)
}

func TestWorkerBeginResponseLostPersistsRecoveryBeforeCancel(t *testing.T) {
	t.Parallel()

	w := newScaleInWorld(2, true)
	w.beginErr = errors.New("begin response lost")
	w.beginApplyThenErr = true
	r := newTestScaleInReconciler(w)
	now := time.Now()
	r.scaleIn.stabilizer.firstObservedAt = now.Add(-time.Second)

	_, err := r.reconcileScaleIn(t.Context(), now, 0)
	require.ErrorContains(t, err, "response lost")
	active := slices.IndexFunc(w.nodes, func(node NomadScaleInNode) bool { return node.Operation != nil })
	require.NotEqual(t, -1, active)
	require.Equal(t, "restoring", w.nodes[active].Operation.Stage)
	require.Zero(t, w.cancelCalls, "recovery ownership must be durable before cancellation")

	w.beginErr = nil
	w.beginApplyThenErr = false
	_, err = r.reconcileScaleIn(t.Context(), now, 0)
	require.NoError(t, err)
	require.Equal(t, "restored", w.nodes[active].Operation.Stage)
	require.Equal(t, 1, w.cancelCalls)
}

func TestRestoringSurvivesCancelAndNomadRestoreResponseLoss(t *testing.T) {
	t.Parallel()

	w := newScaleInWorld(1, true)
	op := NomadScaleInOperation{OperationID: "op", ServiceInstanceID: "service-i-000", StartedAt: time.Now(), Stage: "restoring"}
	w.nodes[0].Eligible, w.nodes[0].Draining, w.nodes[0].Operation = false, true, &op
	w.workers["i-000"] = readyWorker("i-000", op.ServiceInstanceID, op.OperationID)
	w.cancelErr, w.cancelApplyThenErr = errors.New("cancel response lost"), true
	_, err := newTestScaleInReconciler(w).reconcileScaleIn(t.Context(), time.Now(), 1)
	require.ErrorContains(t, err, "response lost")
	require.Equal(t, "restoring", w.nodes[0].Operation.Stage)
	require.Zero(t, w.restoreCalls)
	w.cancelErr, w.cancelApplyThenErr = nil, false
	w.restoreErr, w.restoreApplyThenErr = errors.New("restore response lost"), true
	_, err = newTestScaleInReconciler(w).reconcileScaleIn(t.Context(), time.Now(), 1)
	require.ErrorContains(t, err, "response lost")
	require.True(t, w.nodes[0].Eligible)
	require.False(t, w.nodes[0].Draining)
	require.Equal(t, 1, w.restoreCalls)
	w.restoreErr, w.restoreApplyThenErr = nil, false
	_, err = newTestScaleInReconciler(w).reconcileScaleIn(t.Context(), time.Now(), 1)
	require.NoError(t, err)
	require.Equal(t, "restored", w.nodes[0].Operation.Stage)
	require.Equal(t, 1, w.restoreCalls, "fresh eligible state skips replaying RestoreDrain")
}

func TestSupersededRestoreResponseLossSkipsWorkerAndRestoreReplay(t *testing.T) {
	t.Parallel()

	w := replacementWorld(true, "ready", true)
	w.nodes[0].Operation.Stage = "restoring"
	w.restoreErr, w.restoreApplyThenErr = errors.New("restore response lost"), true
	_, err := newTestScaleInReconciler(w).reconcileScaleIn(t.Context(), time.Now(), 1)
	require.ErrorContains(t, err, "response lost")
	require.True(t, w.nodes[0].Eligible)
	require.False(t, w.nodes[0].Draining)
	require.Zero(t, w.cancelCalls)
	w.restoreErr, w.restoreApplyThenErr = nil, false
	_, err = newTestScaleInReconciler(w).reconcileScaleIn(t.Context(), time.Now(), 1)
	require.NoError(t, err)
	require.Equal(t, "restored", w.nodes[0].Operation.Stage)
	require.Equal(t, 1, w.restoreCalls)
	require.Zero(t, w.cancelCalls)
}

func TestASGUnsettledNeverDecrementsDesired(t *testing.T) {
	t.Parallel()

	w := newScaleInWorld(3, true)
	w.armAll()
	w.restoreOrdinary()
	instance := w.cloud.Instances["i-002"]
	instance.LifecycleState = "Terminating"
	w.cloud.Instances["i-002"] = instance
	_, err := newTestScaleInReconciler(w).reconcileScaleIn(t.Context(), time.Now(), 0)
	require.NoError(t, err)
	require.Empty(t, w.desiredWrites)
}

func TestNewDrainStagePersistenceFailureStopsAndPersistsRecoveryBeforeCancel(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		stageErrors []error
		wantStage   string
	}{
		{"recovery marker succeeds", []error{errors.New("stage write failed"), nil}, "restoring"},
		{"recovery marker fails", []error{errors.New("stage write failed"), errors.New("recovery marker failed")}, "nomad_marked"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			w := newScaleInWorld(2, true)
			w.markStageErrors = tc.stageErrors
			r := newTestScaleInReconciler(w)
			now := time.Now()
			r.scaleIn.stabilizer.firstObservedAt = now.Add(-time.Second)
			_, err := r.reconcileScaleIn(t.Context(), now, 0)
			require.ErrorContains(t, err, "stage write failed")
			require.Equal(t, 1, w.markDrainCalls)
			require.Equal(t, 1, w.beginCalls)
			require.Zero(t, w.cancelCalls)
			active := slices.IndexFunc(w.nodes, func(node NomadScaleInNode) bool { return node.Operation != nil })
			require.NotEqual(t, -1, active)
			require.Equal(t, tc.wantStage, w.nodes[active].Operation.Stage)
		})
	}
}

func TestOperationFailureDoesNotStarveLaterRecovery(t *testing.T) {
	t.Parallel()

	w := newScaleInWorld(3, true)
	w.verifyErrors = map[string]error{"i-000": errors.New("verify failed")}
	first := NomadScaleInOperation{OperationID: "op-0", ServiceInstanceID: "service-i-000", StartedAt: time.Now(), Stage: "worker_draining"}
	second := NomadScaleInOperation{OperationID: "op-1", ServiceInstanceID: "service-i-001", StartedAt: time.Now(), Stage: "restoring"}
	w.nodes[0].Eligible, w.nodes[0].Draining, w.nodes[0].Operation = false, true, &first
	w.nodes[1].Eligible, w.nodes[1].Draining, w.nodes[1].Operation = false, true, &second
	w.workers["i-000"] = readyWorker("i-000", first.ServiceInstanceID, first.OperationID)
	w.workers["i-001"] = readyWorker("i-001", second.ServiceInstanceID, second.OperationID)
	_, err := newTestScaleInReconciler(w).reconcileScaleIn(t.Context(), time.Now(), 3)
	require.ErrorContains(t, err, "verify failed")
	require.Equal(t, "restored", w.nodes[1].Operation.Stage, "independent restoring operation must still converge")
}

func TestRestoredMarkerIsTerminalAndAllowsFutureScaleIn(t *testing.T) {
	t.Parallel()

	op := NomadScaleInOperation{OperationID: "done", ServiceInstanceID: "service", Stage: "restored"}
	node := NomadScaleInNode{Ready: true, Eligible: true, Operation: &op}
	require.False(t, hasActiveScaleInOperation(node))
}

func TestRestoreRequiresExactHealthyWorkerIdentity(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		mutate func(*WorkerScaleInState)
	}{
		{"node", func(s *WorkerScaleInState) { s.NodeID = "other" }},
		{"service", func(s *WorkerScaleInState) { s.ServiceInstanceID = "other" }},
		{"health", func(s *WorkerScaleInState) { s.ServiceStatus = "Unhealthy" }},
		{"protocol", func(s *WorkerScaleInState) { s.ScaleInProtocolSupport = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			w := newScaleInWorld(1, true)
			op := NomadScaleInOperation{OperationID: "op", ServiceInstanceID: "service-i-000", Stage: "restoring"}
			w.nodes[0].Eligible, w.nodes[0].Draining, w.nodes[0].Operation = false, true, &op
			state := readyWorker("i-000", op.ServiceInstanceID, op.OperationID)
			tc.mutate(&state)
			w.cancelState = &state
			_, _, err := newTestScaleInReconciler(w).restoreOne(t.Context(), w.nodes[0], op, w.cloud.Instances["i-000"])
			require.Error(t, err)
			require.Zero(t, w.restoreCalls)
		})
	}
}

func TestScaleInModel500WorkersUsesSettledBatchesOfAtMost50(t *testing.T) {
	t.Parallel()

	w := newScaleInWorld(500, true)
	r := newTestScaleInReconciler(w)
	for w.cloud.DesiredCapacity > 1 {
		batch := min(int(w.cloud.DesiredCapacity-1), int(ScaleInGlobalBudget))
		ids := make([]string, 0, batch)
		for i := range batch {
			id := w.nodes[i].NodeID
			ids = append(ids, id)
			nodeIndex := slices.IndexFunc(w.nodes, func(node NomadScaleInNode) bool { return node.NodeID == id })
			op := NomadScaleInOperation{OperationID: "op-" + id, ServiceInstanceID: "service-" + id, StartedAt: time.Now(), Stage: "worker_draining"}
			w.nodes[nodeIndex].Eligible, w.nodes[nodeIndex].Draining, w.nodes[nodeIndex].Operation = false, true, &op
			instance := w.cloud.Instances[id]
			instance.ProtectedFromScaleIn = false
			w.cloud.Instances[id] = instance
			w.workers[id] = readyWorker(id, op.ServiceInstanceID, op.OperationID)
		}
		before := w.cloud.DesiredCapacity
		_, err := r.reconcileScaleIn(t.Context(), time.Now(), 0)
		require.NoError(t, err)
		require.Equal(t, int32(batch), before-w.cloud.DesiredCapacity)
		for _, id := range ids {
			delete(w.cloud.Instances, id)
		}
		w.nodes = slices.DeleteFunc(w.nodes, func(node NomadScaleInNode) bool {
			_, member := w.cloud.Instances[node.NodeID]

			return !member
		})
	}
	require.Equal(t, []int32{450, 400, 350, 300, 250, 200, 150, 100, 50, 1}, w.desiredWrites)
}

func TestScaleInOpensAndRemovesOneFullEmptyWorkerBatch(t *testing.T) {
	t.Parallel()

	w := newScaleInWorld(50, true)
	r := newTestScaleInReconciler(w)
	now := time.Now()
	w.candidates = make([]ScaleInCandidateObservation, 0, len(w.nodes))
	for _, node := range w.nodes {
		state := w.workers[node.NodeID]
		w.candidates = append(w.candidates, ScaleInCandidateObservation{
			NodeID: node.NodeID, NomadNodeID: node.NomadNodeID, ServiceInstanceID: state.ServiceInstanceID,
			ServiceStatus: "ready", ScaleInProtocolSupport: true, ObservedAt: now,
		})
	}

	result, err := r.reconcileScaleIn(t.Context(), now, 0)
	require.NoError(t, err)
	require.False(t, result.ScaleInStable)
	require.Empty(t, w.desiredWrites)

	result, err = r.reconcileScaleIn(t.Context(), now.Add(time.Second), 0)
	require.NoError(t, err)
	require.Equal(t, int32(49), result.ScaleInDraining)
	require.Equal(t, 49, w.beginCalls)
	require.Empty(t, w.desiredWrites)

	_, err = r.reconcileScaleIn(t.Context(), now.Add(2*time.Second), 0)
	require.NoError(t, err)
	require.Len(t, w.protectionWrites, 1)
	require.Len(t, w.protectionWrites[0].ids, 49)
	require.False(t, w.protectionWrites[0].protected)
	require.Empty(t, w.desiredWrites)

	result, err = r.reconcileScaleIn(t.Context(), now.Add(3*time.Second), 0)
	require.NoError(t, err)
	require.True(t, result.Scaled)
	require.Equal(t, int32(1), result.TargetNodes)
	require.Equal(t, []int32{1}, w.desiredWrites)
}
