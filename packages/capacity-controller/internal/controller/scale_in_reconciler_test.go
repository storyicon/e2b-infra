package controller

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type rollingScaleInWorld struct {
	mu         sync.Mutex
	nodes      []NomadScaleInNode
	instances  map[string]ScaleInASGInstance
	now        time.Time
	opened     int
	peakActive int
}

func (w *rollingScaleInWorld) Inventory(context.Context, string) ([]NomadScaleInNode, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	result := make([]NomadScaleInNode, len(w.nodes))
	copy(result, w.nodes)

	return result, nil
}

func (w *rollingScaleInWorld) MarkDrain(_ context.Context, node NomadScaleInNode, operation NomadScaleInOperation) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for index := range w.nodes {
		if w.nodes[index].NodeID == node.NodeID {
			w.nodes[index].Draining = true
			w.nodes[index].Eligible = false
			w.nodes[index].Operation = &operation
			w.opened++
			w.updatePeakLocked()

			return nil
		}
	}

	return errors.New("node not found")
}

func (w *rollingScaleInWorld) MarkOperationStage(_ context.Context, node NomadScaleInNode, operation NomadScaleInOperation) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for index := range w.nodes {
		if w.nodes[index].NodeID == node.NodeID {
			operationCopy := operation
			w.nodes[index].Operation = &operationCopy
			w.updatePeakLocked()

			return nil
		}
	}

	return errors.New("node not found")
}

func (w *rollingScaleInWorld) RestoreDrain(context.Context, NomadScaleInNode, NomadScaleInOperation) error {
	return errors.New("unexpected rollback")
}

func (w *rollingScaleInWorld) CompleteRestore(context.Context, NomadScaleInNode, NomadScaleInOperation) error {
	return errors.New("unexpected restore completion")
}

func (w *rollingScaleInWorld) CompleteTermination(_ context.Context, node NomadScaleInNode, operation NomadScaleInOperation) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for index := range w.nodes {
		if w.nodes[index].NodeID == node.NodeID {
			operation.Stage = "complete"
			operationCopy := operation
			w.nodes[index].Operation = &operationCopy

			return nil
		}
	}

	return errors.New("node not found")
}

func (w *rollingScaleInWorld) ListScaleInCandidates(context.Context, string) ([]ScaleInCandidateObservation, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	result := make([]ScaleInCandidateObservation, 0, len(w.nodes))
	for _, node := range w.nodes {
		result = append(result, ScaleInCandidateObservation{
			NodeID: node.NodeID, NomadNodeID: node.NomadNodeID,
			ServiceInstanceID: "service-" + node.NodeID,
			ServiceStatus:     "ready", ScaleInProtocolSupport: true, ObservedAt: w.now,
		})
	}

	return result, nil
}

func (w *rollingScaleInWorld) BeginWorkerScaleIn(_ context.Context, _, nodeID, serviceID string) (WorkerScaleInState, error) {
	return readyEmptyWorker(nodeID, serviceID), nil
}

func (w *rollingScaleInWorld) VerifyWorkerScaleIn(_ context.Context, _, nodeID, serviceID string) (WorkerScaleInState, error) {
	return readyEmptyWorker(nodeID, serviceID), nil
}

func (w *rollingScaleInWorld) CancelWorkerScaleIn(context.Context, string, string, string) (WorkerScaleInState, error) {
	return WorkerScaleInState{}, errors.New("unexpected cancellation")
}

func (w *rollingScaleInWorld) Snapshot(context.Context, string) (ScaleInASGSnapshot, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	instances := make(map[string]ScaleInASGInstance, len(w.instances))
	maps.Copy(instances, w.instances)

	return ScaleInASGSnapshot{Name: "workers", MinSize: 0, DesiredCapacity: int32(len(instances)), Instances: instances}, nil
}

func (w *rollingScaleInWorld) TerminateInstance(_ context.Context, instanceID string) (ScaleInTerminationReceipt, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, found := w.instances[instanceID]; !found {
		return ScaleInTerminationReceipt{}, errors.New("instance already departed")
	}
	delete(w.instances, instanceID)

	return ScaleInTerminationReceipt{ActivityID: "activity-" + instanceID}, nil
}

func (w *rollingScaleInWorld) Activity(context.Context, string, string) (*ScaleInActivity, error) {
	return nil, errors.New("unexpected activity lookup")
}

func (w *rollingScaleInWorld) updatePeakLocked() {
	active := 0
	for _, node := range w.nodes {
		if node.Operation != nil && node.Operation.Stage != "complete" {
			active++
		}
	}
	if active > w.peakActive {
		w.peakActive = active
	}
}

func readyEmptyWorker(nodeID, serviceID string) WorkerScaleInState {
	return WorkerScaleInState{
		NodeID: nodeID, ServiceInstanceID: serviceID, ServiceStatus: "Draining",
		ScaleInProtocolSupport: true, ShutdownReady: true, SandboxListEmpty: true,
	}
}

func TestFiveHundredEmptyWorkersConvergeInRollingBatches(t *testing.T) {
	t.Parallel()

	now := time.Unix(1000, 0)
	old := now.Add(-time.Hour)
	world := &rollingScaleInWorld{now: now, instances: make(map[string]ScaleInASGInstance, 500)}
	for index := range 500 {
		id := fmt.Sprintf("i-%03d", index)
		world.nodes = append(world.nodes, NomadScaleInNode{
			NomadNodeID: "nomad-" + id, NodeID: id, NodePool: "workers",
			Ready: true, Eligible: true, CreateIndex: uint64(index),
		})
		world.instances[id] = ScaleInASGInstance{
			ID: id, HealthStatus: "Healthy", LifecycleState: "InService", LaunchTime: &old,
		}
	}
	r := NewWithScaleIn(&Config{
		ClusterID: "cluster-1", NodePool: "workers", ASGName: "workers",
		SlotsPerNode: 20, ScaleInMode: ScaleInModeEnforce, ScaleInStableFor: time.Second,
		ScaleInMinimumAge: 10 * time.Minute, ScaleInTimeout: 15 * time.Minute,
	}, nil, &fakeCapacitySnapshotReader{}, &fakeNodeCounter{}, &fakeScaleTarget{}, world, world, world)
	r.scaleIn.stabilizer.firstObservedAt = now.Add(-time.Minute)

	for range 120 {
		_, err := r.reconcileScaleIn(t.Context(), now, 0, Result{})
		require.NoError(t, err)
		world.mu.Lock()
		remaining := len(world.instances)
		world.mu.Unlock()
		if remaining == 0 {
			break
		}
	}

	world.mu.Lock()
	defer world.mu.Unlock()
	require.Empty(t, world.instances)
	require.Equal(t, 500, world.opened)
	require.LessOrEqual(t, world.peakActive, int(ScaleInGlobalBudget))
}

type scaleInTestInventory struct {
	nodes                    []NomadScaleInNode
	err                      error
	inventoryCalls           int
	markDrainErr             error
	markStageErrors          []error
	markStageCalls           int
	markDrainCalls           int
	restoreErr               error
	restoreCalls             int
	completeCalls            int
	completeTerminationCalls int
}

func (f *scaleInTestInventory) Inventory(context.Context, string) ([]NomadScaleInNode, error) {
	f.inventoryCalls++

	return f.nodes, f.err
}

func (f *scaleInTestInventory) MarkDrain(context.Context, NomadScaleInNode, NomadScaleInOperation) error {
	f.markDrainCalls++

	return f.markDrainErr
}

func (f *scaleInTestInventory) MarkOperationStage(context.Context, NomadScaleInNode, NomadScaleInOperation) error {
	index := f.markStageCalls
	f.markStageCalls++
	if index < len(f.markStageErrors) {
		return f.markStageErrors[index]
	}

	return nil
}

func (f *scaleInTestInventory) RestoreDrain(context.Context, NomadScaleInNode, NomadScaleInOperation) error {
	f.restoreCalls++

	return f.restoreErr
}

func (f *scaleInTestInventory) CompleteRestore(context.Context, NomadScaleInNode, NomadScaleInOperation) error {
	f.completeCalls++

	return nil
}

func (f *scaleInTestInventory) CompleteTermination(context.Context, NomadScaleInNode, NomadScaleInOperation) error {
	f.completeTerminationCalls++

	return nil
}

type scaleInTestWorkers struct {
	candidates  []ScaleInCandidateObservation
	begin       WorkerScaleInState
	beginErr    error
	verify      WorkerScaleInState
	verifyErr   error
	cancelState *WorkerScaleInState
	cancelErr   error
	cancelCalls int
	beginCalls  int
	listCalls   int
}

func (f *scaleInTestWorkers) ListScaleInCandidates(context.Context, string) ([]ScaleInCandidateObservation, error) {
	f.listCalls++

	return f.candidates, nil
}

func (f *scaleInTestWorkers) BeginWorkerScaleIn(context.Context, string, string, string) (WorkerScaleInState, error) {
	f.beginCalls++

	return f.begin, f.beginErr
}

func (f *scaleInTestWorkers) VerifyWorkerScaleIn(context.Context, string, string, string) (WorkerScaleInState, error) {
	return f.verify, f.verifyErr
}

func (f *scaleInTestWorkers) CancelWorkerScaleIn(_ context.Context, _ string, nodeID, serviceID string) (WorkerScaleInState, error) {
	f.cancelCalls++
	if f.cancelErr != nil {
		return WorkerScaleInState{}, f.cancelErr
	}
	if f.cancelState != nil {
		return *f.cancelState, nil
	}

	return WorkerScaleInState{NodeID: nodeID, ServiceInstanceID: serviceID, ServiceStatus: "Healthy", ScaleInProtocolSupport: true}, nil
}

func TestNewDrainRollbackFailureStopsOpeningMoreOperations(t *testing.T) {
	t.Parallel()

	now := time.Unix(1000, 0)
	old := now.Add(-time.Hour)
	inventory := &scaleInTestInventory{
		nodes: []NomadScaleInNode{
			{NomadNodeID: "nomad-1", NodeID: "i-1", NodePool: "workers", Ready: true, Eligible: true},
			{NomadNodeID: "nomad-2", NodeID: "i-2", NodePool: "workers", Ready: true, Eligible: true},
			{NomadNodeID: "nomad-3", NodeID: "i-3", NodePool: "workers", Ready: true, Eligible: true},
		},
		restoreErr: errors.New("Nomad restore failed"),
	}
	workers := &scaleInTestWorkers{
		beginErr: errors.New("worker unavailable"),
		candidates: []ScaleInCandidateObservation{
			{NodeID: "i-1", ServiceInstanceID: "service-1", ServiceStatus: "ready", ScaleInProtocolSupport: true, ObservedAt: now},
			{NodeID: "i-2", ServiceInstanceID: "service-2", ServiceStatus: "ready", ScaleInProtocolSupport: true, ObservedAt: now},
			{NodeID: "i-3", ServiceInstanceID: "service-3", ServiceStatus: "ready", ScaleInProtocolSupport: true, ObservedAt: now},
		},
	}
	infrastructure := &scaleInTestInfrastructure{snapshot: ScaleInASGSnapshot{
		Name: "workers", DesiredCapacity: 3, MinSize: 0,
		Instances: map[string]ScaleInASGInstance{
			"i-1": {ID: "i-1", HealthStatus: "Healthy", LifecycleState: "InService", LaunchTime: &old},
			"i-2": {ID: "i-2", HealthStatus: "Healthy", LifecycleState: "InService", LaunchTime: &old},
			"i-3": {ID: "i-3", HealthStatus: "Healthy", LifecycleState: "InService", LaunchTime: &old},
		},
	}}
	r := NewWithScaleIn(&Config{
		ClusterID: "cluster-1", NodePool: "workers", ASGName: "workers",
		SlotsPerNode: 20, ScaleInMode: ScaleInModeEnforce, ScaleInStableFor: time.Second,
		ScaleInMinimumAge: 10 * time.Minute, ScaleInTimeout: 15 * time.Minute,
	}, nil, &fakeCapacitySnapshotReader{}, &fakeNodeCounter{}, &fakeScaleTarget{}, inventory, workers, infrastructure)
	r.scaleIn.stabilizer.firstObservedAt = now.Add(-time.Minute)

	_, err := r.reconcileScaleIn(t.Context(), now, 0, Result{})

	require.ErrorContains(t, err, "rollback failed")
	require.ErrorContains(t, err, "Nomad restore failed")
	require.Equal(t, 1, inventory.markDrainCalls, "a failed rollback must stop the candidate loop")
	require.Equal(t, 1, workers.beginCalls)
	require.Equal(t, 1, inventory.restoreCalls)
}

func TestNewDrainReportsStagePersistenceFailureAfterSuccessfulRollback(t *testing.T) {
	t.Parallel()

	now := time.Unix(1000, 0)
	old := now.Add(-time.Hour)
	inventory := &scaleInTestInventory{
		nodes: []NomadScaleInNode{
			{NomadNodeID: "nomad-target", NodeID: "i-target", NodePool: "workers", Ready: true, Eligible: true},
			{NomadNodeID: "nomad-keeper", NodeID: "i-keeper", NodePool: "workers", Ready: true, Eligible: true},
		},
		markStageErrors: []error{errors.New("Nomad stage write failed")},
	}
	workers := &scaleInTestWorkers{
		begin: WorkerScaleInState{
			NodeID: "i-target", ServiceInstanceID: "service-target", ServiceStatus: "Draining",
			ScaleInProtocolSupport: true, ShutdownReady: true, SandboxListEmpty: true,
		},
		candidates: []ScaleInCandidateObservation{
			{NodeID: "i-target", ServiceInstanceID: "service-target", ServiceStatus: "ready", ScaleInProtocolSupport: true, ObservedAt: now},
			{NodeID: "i-keeper", ServiceInstanceID: "service-keeper", ServiceStatus: "ready", ScaleInProtocolSupport: false, ObservedAt: now},
		},
	}
	infrastructure := &scaleInTestInfrastructure{snapshot: ScaleInASGSnapshot{
		Name: "workers", DesiredCapacity: 2, MinSize: 1,
		Instances: map[string]ScaleInASGInstance{
			"i-target": {ID: "i-target", HealthStatus: "Healthy", LifecycleState: "InService", LaunchTime: &old},
			"i-keeper": {ID: "i-keeper", HealthStatus: "Healthy", LifecycleState: "InService", LaunchTime: &old},
		},
	}}
	r := NewWithScaleIn(&Config{
		ClusterID: "cluster-1", NodePool: "workers", ASGName: "workers",
		SlotsPerNode: 20, MinNodes: 1, ScaleInMode: ScaleInModeEnforce,
		ScaleInStableFor: time.Second, ScaleInMinimumAge: 10 * time.Minute, ScaleInTimeout: 15 * time.Minute,
	}, nil, &fakeCapacitySnapshotReader{}, &fakeNodeCounter{}, &fakeScaleTarget{}, inventory, workers, infrastructure)
	r.scaleIn.stabilizer.firstObservedAt = now.Add(-time.Minute)

	_, err := r.reconcileScaleIn(t.Context(), now, 0, Result{})

	require.ErrorContains(t, err, "persist worker drain stage")
	require.ErrorContains(t, err, "Nomad stage write failed")
	require.Equal(t, 1, inventory.restoreCalls)
	require.Equal(t, 1, workers.cancelCalls)
	require.Equal(t, 1, inventory.completeCalls)
}

func TestNewDrainSkipsInfrastructureBlockedCandidatesBeforeMutation(t *testing.T) {
	t.Parallel()

	now := time.Unix(1000, 0)
	old := now.Add(-time.Hour)
	for _, test := range []struct {
		name               string
		protected          bool
		terminateSuspended bool
		refreshActive      bool
		wantDrain          int
	}{
		{name: "allowed", wantDrain: 1},
		{name: "instance protected", protected: true},
		{name: "terminate suspended", terminateSuspended: true},
		{name: "instance refresh active", refreshActive: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			inventory := &scaleInTestInventory{nodes: []NomadScaleInNode{
				{NomadNodeID: "nomad-target", NodeID: "i-target", NodePool: "workers", Ready: true, Eligible: true, CreateIndex: 2},
				{NomadNodeID: "nomad-keeper", NodeID: "i-keeper", NodePool: "workers", Ready: true, Eligible: true, CreateIndex: 1},
			}}
			workers := &scaleInTestWorkers{
				begin: WorkerScaleInState{NodeID: "i-target", ServiceInstanceID: "service-target", ServiceStatus: "Draining", ScaleInProtocolSupport: true, ShutdownReady: true, SandboxListEmpty: true},
				candidates: []ScaleInCandidateObservation{
					{NodeID: "i-target", ServiceInstanceID: "service-target", ServiceStatus: "ready", ScaleInProtocolSupport: true, ObservedAt: now},
					{NodeID: "i-keeper", ServiceInstanceID: "service-keeper", ServiceStatus: "ready", ScaleInProtocolSupport: false, ObservedAt: now},
				},
			}
			infrastructure := &scaleInTestInfrastructure{snapshot: ScaleInASGSnapshot{
				Name: "workers", DesiredCapacity: 2, MinSize: 1,
				TerminateSuspended: test.terminateSuspended, ActiveInstanceRefresh: test.refreshActive,
				Instances: map[string]ScaleInASGInstance{
					"i-target": {ID: "i-target", HealthStatus: "Healthy", LifecycleState: "InService", ProtectedFromScaleIn: test.protected, LaunchTime: &old},
					"i-keeper": {ID: "i-keeper", HealthStatus: "Healthy", LifecycleState: "InService", LaunchTime: &old},
				},
			}}
			r := NewWithScaleIn(&Config{
				ClusterID: "cluster-1", NodePool: "workers", ASGName: "workers",
				SlotsPerNode: 20, MinNodes: 1, ScaleInMode: ScaleInModeEnforce,
				ScaleInStableFor: time.Second, ScaleInMinimumAge: 10 * time.Minute, ScaleInTimeout: 15 * time.Minute,
			}, nil, &fakeCapacitySnapshotReader{}, &fakeNodeCounter{}, &fakeScaleTarget{}, inventory, workers, infrastructure)
			r.scaleIn.stabilizer.firstObservedAt = now.Add(-time.Minute)

			_, err := r.reconcileScaleIn(t.Context(), now, 0, Result{})

			require.NoError(t, err)
			require.Equal(t, test.wantDrain, inventory.markDrainCalls)
			require.Equal(t, test.wantDrain, workers.beginCalls)
		})
	}
}

type scaleInTestInfrastructure struct {
	snapshot       ScaleInASGSnapshot
	receipt        ScaleInTerminationReceipt
	terminationErr error
	terminateCalls int
	activity       *ScaleInActivity
	activityErr    error
	snapshotCalls  int
}

func (f *scaleInTestInfrastructure) Snapshot(context.Context, string) (ScaleInASGSnapshot, error) {
	f.snapshotCalls++

	return f.snapshot, nil
}

func TestScaleInOffDoesNotTouchScaleInDependencies(t *testing.T) {
	t.Parallel()

	inventory := &scaleInTestInventory{}
	workers := &scaleInTestWorkers{}
	infrastructure := &scaleInTestInfrastructure{}
	r := NewWithScaleIn(&Config{ScaleInMode: ScaleInModeOff}, nil, nil, nil, nil, inventory, workers, infrastructure)

	result, err := r.reconcileScaleIn(t.Context(), time.Unix(100, 0), 0, Result{})

	require.NoError(t, err)
	require.Equal(t, Result{}, result)
	require.Zero(t, inventory.inventoryCalls)
	require.Zero(t, workers.listCalls)
	require.Zero(t, infrastructure.snapshotCalls)
	require.Zero(t, inventory.markDrainCalls)
	require.Zero(t, infrastructure.terminateCalls)
}

func TestObserveMixedWorkerVersionsNeverMutatesCapacity(t *testing.T) {
	t.Parallel()

	now := time.Unix(1000, 0)
	old := now.Add(-time.Hour)
	inventory := &scaleInTestInventory{nodes: []NomadScaleInNode{
		{NodeID: "i-new", Ready: true, Eligible: true},
		{NodeID: "i-old", Ready: true, Eligible: true},
	}}
	workers := &scaleInTestWorkers{candidates: []ScaleInCandidateObservation{
		{NodeID: "i-new", ServiceInstanceID: "service-new", ServiceStatus: "ready", ScaleInProtocolSupport: true, ObservedAt: now},
		{NodeID: "i-old", ServiceInstanceID: "service-old", ServiceStatus: "ready", ScaleInProtocolSupport: false, ObservedAt: now},
	}}
	infrastructure := &scaleInTestInfrastructure{snapshot: ScaleInASGSnapshot{Instances: map[string]ScaleInASGInstance{
		"i-new": {ID: "i-new", HealthStatus: "Healthy", LifecycleState: "InService", LaunchTime: &old},
		"i-old": {ID: "i-old", HealthStatus: "Healthy", LifecycleState: "InService", LaunchTime: &old},
	}}}
	r := NewWithScaleIn(&Config{
		ScaleInMode: ScaleInModeObserve, SlotsPerNode: 20, MinNodes: 1,
		ScaleInStableFor: time.Second,
	}, nil, nil, nil, nil, inventory, workers, infrastructure)

	result, err := r.reconcileScaleIn(t.Context(), now, 0, Result{})

	require.NoError(t, err)
	require.Equal(t, int32(2), result.ScaleInAccepting, "legacy workers still provide create capacity during rollout")
	merged := mergeScaleInNodes(now, inventory.nodes, workers.candidates, infrastructure.snapshot)
	eligible := EligibleScaleInCandidates(merged, now, 0)
	require.Len(t, eligible, 1)
	require.Equal(t, "i-new", eligible[0].NodeID, "only the protocol-capable worker may become a scale-in candidate")
	require.Zero(t, inventory.markDrainCalls)
	require.Zero(t, workers.beginCalls)
	require.Zero(t, infrastructure.terminateCalls)
}

func (f *scaleInTestInfrastructure) TerminateInstance(context.Context, string) (ScaleInTerminationReceipt, error) {
	f.terminateCalls++

	return f.receipt, f.terminationErr
}

func TestCandidateObservationFreshnessFailsClosed(t *testing.T) {
	t.Parallel()

	now := time.Unix(100, 0)
	nomad := []NomadScaleInNode{{NodeID: "i-1", Ready: true, Eligible: true}}
	cloud := ScaleInASGSnapshot{Instances: map[string]ScaleInASGInstance{"i-1": {ID: "i-1", HealthStatus: "Healthy", LifecycleState: "InService"}}}
	base := ScaleInCandidateObservation{NodeID: "i-1", ServiceStatus: "ready", ScaleInProtocolSupport: true}

	for _, test := range []struct {
		name       string
		observedAt time.Time
		wantReady  bool
	}{
		{name: "fresh", observedAt: now.Add(-scaleInCandidateMaxAge), wantReady: true},
		{name: "expired", observedAt: now.Add(-scaleInCandidateMaxAge - time.Nanosecond)},
		{name: "future", observedAt: now.Add(time.Nanosecond)},
		{name: "missing"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			candidate := base
			candidate.ObservedAt = test.observedAt
			nodes := mergeScaleInNodes(now, nomad, []ScaleInCandidateObservation{candidate}, cloud)
			require.Equal(t, test.wantReady, nodes[0].Healthy)
			require.Equal(t, test.wantReady, nodes[0].ScaleInProtocolSupport)
		})
	}
}

func TestFinishCancelRequiresHealthyProtocolCapableIdentity(t *testing.T) {
	t.Parallel()

	operation := NomadScaleInOperation{OperationID: "op-1", ServiceInstanceID: "service-1", Stage: "restoring"}
	node := NomadScaleInNode{NodeID: "i-1"}
	valid := WorkerScaleInState{NodeID: "i-1", ServiceInstanceID: "service-1", ServiceStatus: "Healthy", ScaleInProtocolSupport: true}
	for _, test := range []struct {
		name  string
		state WorkerScaleInState
		ok    bool
	}{
		{name: "valid", state: valid, ok: true},
		{name: "healthy replacement", state: func() WorkerScaleInState {
			value := valid
			value.ServiceInstanceID = "service-2"

			return value
		}(), ok: true},
		{name: "wrong status", state: func() WorkerScaleInState {
			value := valid
			value.ServiceStatus = "Draining"

			return value
		}()},
		{name: "protocol missing", state: func() WorkerScaleInState {
			value := valid
			value.ScaleInProtocolSupport = false

			return value
		}()},
		{name: "missing identity", state: func() WorkerScaleInState {
			value := valid
			value.ServiceInstanceID = ""

			return value
		}()},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			inventory := &scaleInTestInventory{}
			workers := &scaleInTestWorkers{cancelState: &test.state}
			r := NewWithScaleIn(&Config{ClusterID: "cluster-1"}, nil, nil, nil, nil, inventory, workers, &scaleInTestInfrastructure{})
			err := r.finishCancel(t.Context(), node, operation)
			if test.ok {
				require.NoError(t, err)
				require.Equal(t, 1, inventory.completeCalls)
			} else {
				require.Error(t, err)
				require.Zero(t, inventory.completeCalls)
			}
		})
	}
}

func TestActiveScaleInOperationsRetainsInterruptedRestoredMarker(t *testing.T) {
	t.Parallel()

	restored := NomadScaleInOperation{OperationID: "restored", Stage: "restored"}
	complete := NomadScaleInOperation{OperationID: "complete", Stage: "complete"}
	operations := activeScaleInOperations([]NomadScaleInNode{
		{NodeID: "terminal", Eligible: true, Operation: &restored},
		{NodeID: "interrupted", Draining: true, Operation: &restored},
		{NodeID: "terminated", Draining: true, Operation: &complete},
	})

	require.Len(t, operations, 1)
	require.Equal(t, "interrupted", operations[0].NodeID)
}

func TestTerminalRestoreDoesNotBlockFutureScaleIn(t *testing.T) {
	t.Parallel()

	now := time.Unix(1000, 0)
	old := now.Add(-time.Hour)
	restored := NomadScaleInOperation{OperationID: "previous", Stage: "restored"}
	inventory := &scaleInTestInventory{nodes: []NomadScaleInNode{
		{NomadNodeID: "nomad-target", NodeID: "i-target", NodePool: "workers", Ready: true, Eligible: true, Operation: &restored},
		{NomadNodeID: "nomad-keeper", NodeID: "i-keeper", NodePool: "workers", Ready: true, Eligible: true},
	}}
	workers := &scaleInTestWorkers{
		begin: WorkerScaleInState{
			NodeID: "i-target", ServiceInstanceID: "service-target", ServiceStatus: "Draining",
			ScaleInProtocolSupport: true, ShutdownReady: true, SandboxListEmpty: true,
		},
		candidates: []ScaleInCandidateObservation{
			{NodeID: "i-target", ServiceInstanceID: "service-target", ServiceStatus: "ready", ScaleInProtocolSupport: true, ObservedAt: now},
			{NodeID: "i-keeper", ServiceInstanceID: "service-keeper", ServiceStatus: "ready", ScaleInProtocolSupport: false, ObservedAt: now},
		},
	}
	infrastructure := &scaleInTestInfrastructure{snapshot: ScaleInASGSnapshot{
		Name: "workers", DesiredCapacity: 2, MinSize: 1,
		Instances: map[string]ScaleInASGInstance{
			"i-target": {ID: "i-target", HealthStatus: "Healthy", LifecycleState: "InService", LaunchTime: &old},
			"i-keeper": {ID: "i-keeper", HealthStatus: "Healthy", LifecycleState: "InService", LaunchTime: &old},
		},
	}}
	r := NewWithScaleIn(&Config{
		ClusterID: "cluster-1", NodePool: "workers", ASGName: "workers",
		SlotsPerNode: 20, MinNodes: 1, ScaleInMode: ScaleInModeEnforce,
		ScaleInStableFor: time.Second, ScaleInMinimumAge: 10 * time.Minute, ScaleInTimeout: 15 * time.Minute,
	}, nil, &fakeCapacitySnapshotReader{}, &fakeNodeCounter{}, &fakeScaleTarget{}, inventory, workers, infrastructure)
	r.scaleIn.stabilizer.firstObservedAt = now.Add(-time.Minute)

	result, err := r.reconcileScaleIn(t.Context(), now, 0, Result{})

	require.NoError(t, err)
	require.Equal(t, int32(1), result.ScaleInDraining)
	require.Equal(t, 1, inventory.markDrainCalls)
	require.Equal(t, 1, workers.beginCalls)
}

func TestDerivedRestoringOperationOnlyFinishesCancellation(t *testing.T) {
	t.Parallel()

	now := time.Unix(100, 0)
	operation := NomadScaleInOperation{
		OperationID:       "op-1",
		ServiceInstanceID: "service-1",
		StartedAt:         now.Add(-time.Minute),
		Stage:             "restoring",
	}
	inventory := &scaleInTestInventory{nodes: []NomadScaleInNode{{
		NomadNodeID: "nomad-1",
		NodeID:      "i-1",
		NodePool:    "workers",
		Ready:       true,
		Eligible:    true,
		Operation:   &operation,
	}}}
	state := WorkerScaleInState{
		NodeID:                 "i-1",
		ServiceInstanceID:      "service-1",
		ServiceStatus:          "Healthy",
		ScaleInProtocolSupport: true,
	}
	workers := &scaleInTestWorkers{
		candidates: []ScaleInCandidateObservation{{
			NodeID:                 "i-1",
			ServiceInstanceID:      "service-1",
			ServiceStatus:          "ready",
			ScaleInProtocolSupport: true,
			ObservedAt:             now,
		}},
		cancelState: &state,
	}
	infrastructure := &scaleInTestInfrastructure{snapshot: ScaleInASGSnapshot{
		Name:            "workers",
		DesiredCapacity: 1,
		MinSize:         1,
		Instances: map[string]ScaleInASGInstance{
			"i-1": {ID: "i-1", HealthStatus: "Healthy", LifecycleState: "InService"},
		},
	}}
	r := NewWithScaleIn(&Config{
		ClusterID: "cluster-1", NodePool: "workers", ASGName: "workers",
		SlotsPerNode: 20, MinNodes: 1, ScaleInMode: ScaleInModeEnforce,
	}, nil, nil, nil, nil, inventory, workers, infrastructure)

	result, err := r.reconcileScaleIn(t.Context(), now, 20, Result{})

	require.NoError(t, err)
	require.Equal(t, int32(1), result.ScaleInCancelled)
	require.Equal(t, 1, workers.cancelCalls)
	require.Equal(t, 1, inventory.completeCalls)
	require.Zero(t, infrastructure.terminateCalls)
}

func TestNomadMarkedRecoveryBeginsWorkerImmediately(t *testing.T) {
	t.Parallel()

	operation := NomadScaleInOperation{OperationID: "op-1", ServiceInstanceID: "service-1", Stage: "nomad_marked"}
	node := NomadScaleInNode{NodeID: "i-1", Operation: &operation}
	inventory := &scaleInTestInventory{}
	workers := &scaleInTestWorkers{begin: WorkerScaleInState{NodeID: "i-1", ServiceInstanceID: "service-1", ServiceStatus: "Draining", ScaleInProtocolSupport: true}}
	r := NewWithScaleIn(&Config{ClusterID: "cluster-1"}, nil, nil, nil, nil, inventory, workers, &scaleInTestInfrastructure{})

	require.NoError(t, r.resumeNomadMarked(t.Context(), node, operation))
	require.Equal(t, 1, inventory.markStageCalls)
}

func TestNomadMarkedRecoveryCancelsImmediatelyWhenBeginFails(t *testing.T) {
	t.Parallel()

	operation := NomadScaleInOperation{OperationID: "op-1", ServiceInstanceID: "service-1", Stage: "nomad_marked"}
	node := NomadScaleInNode{NodeID: "i-1", Operation: &operation}
	inventory := &scaleInTestInventory{}
	workers := &scaleInTestWorkers{beginErr: errors.New("worker unavailable")}
	r := NewWithScaleIn(&Config{ClusterID: "cluster-1"}, nil, nil, nil, nil, inventory, workers, &scaleInTestInfrastructure{})

	err := r.resumeNomadMarked(t.Context(), node, operation)
	require.ErrorContains(t, err, "worker unavailable")
	require.Equal(t, 1, inventory.restoreCalls)
	require.Equal(t, 1, workers.cancelCalls)
	require.Equal(t, 1, inventory.completeCalls)
}

func TestCommittedReconciliationIsVisibleAndCompletesDepartedInstance(t *testing.T) {
	t.Parallel()

	operation := NomadScaleInOperation{OperationID: "op-1", ServiceInstanceID: "service-1", Stage: "terminating"}
	node := NomadScaleInNode{NodeID: "i-1", Operation: &operation}
	inventory := &scaleInTestInventory{}
	r := NewWithScaleIn(&Config{ASGName: "workers"}, nil, nil, nil, nil, inventory, &scaleInTestWorkers{}, &scaleInTestInfrastructure{})

	err := r.reconcileCommitted(t.Context(), node, operation, ScaleInASGSnapshot{Instances: map[string]ScaleInASGInstance{"i-1": {ID: "i-1"}}})
	require.ErrorContains(t, err, "no activity ID")
	require.NoError(t, r.reconcileCommitted(t.Context(), node, operation, ScaleInASGSnapshot{Instances: map[string]ScaleInASGInstance{}}))
	require.Equal(t, 1, inventory.completeTerminationCalls)
}

func (f *scaleInTestInfrastructure) Activity(context.Context, string, string) (*ScaleInActivity, error) {
	return f.activity, f.activityErr
}

func TestEnforceScaleOutContinuesWhenScaleInObservationFails(t *testing.T) {
	t.Parallel()

	snapshot := &fakeCapacitySnapshotReader{snapshot: CapacitySnapshot{WorkloadCount: 500}}
	target := &fakeScaleTarget{desired: 1}
	inventory := &scaleInTestInventory{err: errors.New("Nomad unavailable")}
	config := &Config{
		Mode: ModeStartIntentV1, ClusterID: "cluster-1", NodePool: "workers", ASGName: "workers",
		SlotsPerNode: 20, MinNodes: 1, MaxNodes: 30, BatchIdleDuration: time.Second,
		BatchMaxDuration: 10 * time.Second, ReconcileTimeout: time.Second, ScaleInMode: ScaleInModeEnforce,
	}
	r := NewWithScaleIn(config, nil, snapshot, &fakeNodeCounter{ready: 1}, target, inventory, &scaleInTestWorkers{}, &scaleInTestInfrastructure{})
	t0 := time.Unix(100, 0)

	first, err := r.Reconcile(t.Context(), t0)
	require.NoError(t, err)
	require.True(t, first.Aggregating)
	require.ErrorContains(t, first.ScaleInReadError, "Nomad unavailable")
	second, err := r.Reconcile(t.Context(), t0.Add(time.Second))
	require.NoError(t, err)
	require.True(t, second.Scaled)
	require.Equal(t, []int32{25}, target.sets)
}

func TestScaleOutCompensatesOnlyUncommittedTerminatingOperations(t *testing.T) {
	t.Parallel()

	now := time.Unix(100, 0)
	for _, test := range []struct {
		name       string
		activityID string
		want       int64
	}{
		{name: "accepted termination", activityID: "activity-1", want: 5},
		{name: "ambiguous termination", want: 6},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			operation := NomadScaleInOperation{
				OperationID:       "op-1",
				ServiceInstanceID: "service-1",
				Stage:             "terminating",
				ActivityID:        test.activityID,
			}
			inventory := &scaleInTestInventory{nodes: []NomadScaleInNode{{
				NodeID:    "i-1",
				NodePool:  "workers",
				Ready:     true,
				Draining:  true,
				Operation: &operation,
			}}}
			workers := &scaleInTestWorkers{candidates: []ScaleInCandidateObservation{{
				NodeID:                 "i-1",
				ServiceInstanceID:      "service-1",
				ServiceStatus:          "draining",
				ScaleInProtocolSupport: true,
				ObservedAt:             now,
			}}}
			infrastructure := &scaleInTestInfrastructure{snapshot: ScaleInASGSnapshot{
				Name:            "workers",
				DesiredCapacity: 5,
				MinSize:         1,
				Instances: map[string]ScaleInASGInstance{
					"i-1": {ID: "i-1", HealthStatus: "Healthy", LifecycleState: "InService"},
				},
			}}
			r := NewWithScaleIn(&Config{
				ClusterID: "cluster-1", NodePool: "workers", ASGName: "workers",
				SlotsPerNode: 20, MinNodes: 1, ScaleInMode: ScaleInModeEnforce,
			}, nil, nil, nil, nil, inventory, workers, infrastructure)

			got, err := r.scaleOutRequiredForEnforce(t.Context(), now, 100, 5)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

func TestCommitTerminationRestoresOnlyAfterExplicitAWSRejection(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name         string
		outcome      ScaleInTerminationErrorOutcome
		wantRestored bool
	}{
		{name: "explicit rejection", outcome: ScaleInTerminationRejected, wantRestored: true},
		{name: "ambiguous transport outcome", outcome: ScaleInTerminationAmbiguous, wantRestored: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			now := time.Unix(100, 0)
			operation := NomadScaleInOperation{OperationID: "op-1", ServiceInstanceID: "service-1", StartedAt: now, Stage: "worker_draining"}
			targetNode := NomadScaleInNode{NomadNodeID: "nomad-target", NodeID: "i-target", NodePool: "workers", Ready: true, Draining: true, Operation: &operation}
			keeperNode := NomadScaleInNode{NomadNodeID: "nomad-keeper", NodeID: "i-keeper", NodePool: "workers", Ready: true, Eligible: true}
			inventory := &scaleInTestInventory{nodes: []NomadScaleInNode{targetNode, keeperNode}}
			workers := &scaleInTestWorkers{
				verify: WorkerScaleInState{NodeID: "i-target", ServiceInstanceID: "service-1", ServiceStatus: "Draining", ScaleInProtocolSupport: true, ShutdownReady: true, SandboxListEmpty: true},
				candidates: []ScaleInCandidateObservation{
					{NodeID: "i-target", ServiceInstanceID: "service-1", ServiceStatus: "draining", ScaleInProtocolSupport: true, ObservedAt: now},
					{NodeID: "i-keeper", ServiceInstanceID: "service-2", ServiceStatus: "ready", ScaleInProtocolSupport: true, ObservedAt: now},
				},
			}
			infrastructure := &scaleInTestInfrastructure{
				snapshot: ScaleInASGSnapshot{Name: "workers", DesiredCapacity: 2, MinSize: 1, Instances: map[string]ScaleInASGInstance{
					"i-target": {ID: "i-target", HealthStatus: "Healthy", LifecycleState: "InService"},
					"i-keeper": {ID: "i-keeper", HealthStatus: "Healthy", LifecycleState: "InService"},
				}},
				terminationErr: &ScaleInTerminationError{Outcome: test.outcome, Err: errors.New("AWS termination failed")},
			}
			r := NewWithScaleIn(&Config{ClusterID: "cluster-1", NodePool: "workers", ASGName: "workers", SlotsPerNode: 20, MinNodes: 1}, nil,
				&fakeCapacitySnapshotReader{snapshot: CapacitySnapshot{}}, &fakeNodeCounter{}, &fakeScaleTarget{}, inventory, workers, infrastructure)

			accepted, err := r.commitTermination(t.Context(), now, targetNode, operation)
			if test.wantRestored {
				require.False(t, accepted)
				require.ErrorContains(t, err, "AWS termination failed")
				require.Equal(t, 1, inventory.restoreCalls)
				require.Equal(t, 1, workers.cancelCalls)
				require.Equal(t, 1, inventory.completeCalls)
			} else {
				require.False(t, accepted)
				require.NoError(t, err)
				require.Zero(t, inventory.restoreCalls)
				require.Zero(t, workers.cancelCalls)
			}
			require.Equal(t, 1, infrastructure.terminateCalls)
		})
	}
}

func TestCommitTerminationReportsAcceptedActivityPersistenceFailure(t *testing.T) {
	t.Parallel()

	now := time.Unix(100, 0)
	operation := NomadScaleInOperation{OperationID: "op-1", ServiceInstanceID: "service-1", StartedAt: now, Stage: "worker_draining"}
	targetNode := NomadScaleInNode{NomadNodeID: "nomad-target", NodeID: "i-target", NodePool: "workers", Ready: true, Draining: true, Operation: &operation}
	keeperNode := NomadScaleInNode{NomadNodeID: "nomad-keeper", NodeID: "i-keeper", NodePool: "workers", Ready: true, Eligible: true}
	inventory := &scaleInTestInventory{
		nodes:           []NomadScaleInNode{targetNode, keeperNode},
		markStageErrors: []error{nil, errors.New("Nomad write failed")},
	}
	workers := &scaleInTestWorkers{
		verify: WorkerScaleInState{NodeID: "i-target", ServiceInstanceID: "service-1", ServiceStatus: "Draining", ScaleInProtocolSupport: true, ShutdownReady: true, SandboxListEmpty: true},
		candidates: []ScaleInCandidateObservation{
			{NodeID: "i-target", ServiceInstanceID: "service-1", ServiceStatus: "draining", ScaleInProtocolSupport: true, ObservedAt: now},
			{NodeID: "i-keeper", ServiceInstanceID: "service-2", ServiceStatus: "ready", ScaleInProtocolSupport: true, ObservedAt: now},
		},
	}
	infrastructure := &scaleInTestInfrastructure{
		snapshot: ScaleInASGSnapshot{Name: "workers", DesiredCapacity: 2, MinSize: 1, Instances: map[string]ScaleInASGInstance{
			"i-target": {ID: "i-target", HealthStatus: "Healthy", LifecycleState: "InService"},
			"i-keeper": {ID: "i-keeper", HealthStatus: "Healthy", LifecycleState: "InService"},
		}},
		receipt: ScaleInTerminationReceipt{ActivityID: "activity-1"},
	}
	r := NewWithScaleIn(&Config{ClusterID: "cluster-1", NodePool: "workers", ASGName: "workers", SlotsPerNode: 20, MinNodes: 1}, nil,
		&fakeCapacitySnapshotReader{snapshot: CapacitySnapshot{}}, &fakeNodeCounter{}, &fakeScaleTarget{}, inventory, workers, infrastructure)

	accepted, err := r.commitTermination(t.Context(), now, targetNode, operation)
	require.False(t, accepted)
	require.ErrorContains(t, err, "persist accepted AWS activity")
	require.Equal(t, 1, infrastructure.terminateCalls)
	require.Equal(t, 2, inventory.markStageCalls)
}

func TestOperationFailureDoesNotStarveLaterRecovery(t *testing.T) {
	t.Parallel()

	now := time.Unix(100, 0)
	firstOperation := NomadScaleInOperation{OperationID: "op-ambiguous", ServiceInstanceID: "service-1", StartedAt: now, Stage: "terminating"}
	secondOperation := NomadScaleInOperation{OperationID: "op-departed", ServiceInstanceID: "service-2", StartedAt: now, Stage: "terminating"}
	inventory := &scaleInTestInventory{nodes: []NomadScaleInNode{
		{NomadNodeID: "nomad-1", NodeID: "i-present", NodePool: "workers", Ready: true, Draining: true, Operation: &firstOperation},
		{NomadNodeID: "nomad-2", NodeID: "i-departed", NodePool: "workers", Ready: true, Draining: true, Operation: &secondOperation},
		{NomadNodeID: "nomad-3", NodeID: "i-keeper", NodePool: "workers", Ready: true, Eligible: true},
	}}
	workers := &scaleInTestWorkers{candidates: []ScaleInCandidateObservation{
		{NodeID: "i-present", ServiceInstanceID: "service-1", ServiceStatus: "draining", ScaleInProtocolSupport: true, ObservedAt: now},
		{NodeID: "i-departed", ServiceInstanceID: "service-2", ServiceStatus: "draining", ScaleInProtocolSupport: true, ObservedAt: now},
		{NodeID: "i-keeper", ServiceInstanceID: "service-3", ServiceStatus: "ready", ScaleInProtocolSupport: true, ObservedAt: now},
	}}
	infrastructure := &scaleInTestInfrastructure{snapshot: ScaleInASGSnapshot{Name: "workers", DesiredCapacity: 2, MinSize: 1, Instances: map[string]ScaleInASGInstance{
		"i-present": {ID: "i-present", HealthStatus: "Healthy", LifecycleState: "InService"},
		"i-keeper":  {ID: "i-keeper", HealthStatus: "Healthy", LifecycleState: "InService"},
	}}}
	r := NewWithScaleIn(&Config{
		ClusterID: "cluster-1", NodePool: "workers", ASGName: "workers", SlotsPerNode: 20, MinNodes: 1,
		ScaleInMode: ScaleInModeEnforce, ScaleInTimeout: 15 * time.Minute,
	}, nil, &fakeCapacitySnapshotReader{}, &fakeNodeCounter{}, &fakeScaleTarget{}, inventory, workers, infrastructure)

	_, err := r.reconcileScaleIn(t.Context(), now, 0, Result{})
	require.ErrorContains(t, err, "no activity ID")
	require.Equal(t, 1, inventory.completeTerminationCalls)
}

func TestOperationFailureDoesNotBlockOpeningAnotherSafeDrain(t *testing.T) {
	t.Parallel()

	now := time.Unix(1000, 0)
	old := now.Add(-time.Hour)
	operation := NomadScaleInOperation{OperationID: "op-ambiguous", ServiceInstanceID: "service-old", StartedAt: old, Stage: "terminating"}
	inventory := &scaleInTestInventory{nodes: []NomadScaleInNode{
		{NomadNodeID: "nomad-old", NodeID: "i-old", NodePool: "workers", Ready: true, Draining: true, Operation: &operation},
		{NomadNodeID: "nomad-keeper", NodeID: "i-keeper", NodePool: "workers", Ready: true, Eligible: true},
		{NomadNodeID: "nomad-new", NodeID: "i-new", NodePool: "workers", Ready: true, Eligible: true},
	}}
	workers := &scaleInTestWorkers{
		begin: WorkerScaleInState{NodeID: "i-new", ServiceInstanceID: "service-new", ServiceStatus: "Draining", ScaleInProtocolSupport: true, ShutdownReady: true, SandboxListEmpty: true},
		candidates: []ScaleInCandidateObservation{
			{NodeID: "i-old", ServiceInstanceID: "service-old", ServiceStatus: "draining", ScaleInProtocolSupport: true, ObservedAt: now},
			{NodeID: "i-keeper", ServiceInstanceID: "service-keeper", ServiceStatus: "ready", RunningSandboxes: 1, ScaleInProtocolSupport: true, ObservedAt: now},
			{NodeID: "i-new", ServiceInstanceID: "service-new", ServiceStatus: "ready", ScaleInProtocolSupport: true, ObservedAt: now},
		},
	}
	infrastructure := &scaleInTestInfrastructure{snapshot: ScaleInASGSnapshot{Name: "workers", DesiredCapacity: 3, MinSize: 1, Instances: map[string]ScaleInASGInstance{
		"i-old":    {ID: "i-old", HealthStatus: "Healthy", LifecycleState: "InService", LaunchTime: &old},
		"i-keeper": {ID: "i-keeper", HealthStatus: "Healthy", LifecycleState: "InService", LaunchTime: &old},
		"i-new":    {ID: "i-new", HealthStatus: "Healthy", LifecycleState: "InService", LaunchTime: &old},
	}}}
	r := NewWithScaleIn(&Config{
		ClusterID: "cluster-1", NodePool: "workers", ASGName: "workers", SlotsPerNode: 20, MinNodes: 1,
		ScaleInMode: ScaleInModeEnforce, ScaleInStableFor: time.Second, ScaleInMinimumAge: 10 * time.Minute, ScaleInTimeout: 15 * time.Minute,
	}, nil, &fakeCapacitySnapshotReader{}, &fakeNodeCounter{}, &fakeScaleTarget{}, inventory, workers, infrastructure)
	r.scaleIn.stabilizer.firstObservedAt = now.Add(-time.Minute)

	result, err := r.reconcileScaleIn(t.Context(), now, 0, Result{})
	require.ErrorContains(t, err, "no activity ID")
	require.Equal(t, int32(1), result.ScaleInDraining)
	require.Equal(t, 1, workers.beginCalls)
}

func TestDepartedNomadNodesDoNotConsumeDisruptionBudget(t *testing.T) {
	t.Parallel()

	now := time.Unix(1000, 0)
	complete := NomadScaleInOperation{OperationID: "op-complete", ServiceInstanceID: "service-old", Stage: "complete"}
	terminating := NomadScaleInOperation{OperationID: "op-terminating", ServiceInstanceID: "service-leaving", Stage: "terminating"}
	nodes := mergeScaleInNodes(now,
		[]NomadScaleInNode{
			{NodeID: "i-stale", Ready: false, Eligible: true},
			{NodeID: "i-complete", Ready: false, Draining: true, Operation: &complete},
			{NodeID: "i-terminating", Ready: false, Draining: true, Operation: &terminating},
		},
		nil,
		ScaleInASGSnapshot{Instances: map[string]ScaleInASGInstance{}},
	)

	require.Empty(t, nodes)
}
