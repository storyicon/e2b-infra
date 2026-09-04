package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
)

const (
	scaleInCandidateMaxAge = 40 * time.Second
)

type scaleInRuntime struct {
	inventory       ScaleInNodeInventory
	workers         ScaleInWorkerControl
	infrastructure  ScaleInInfrastructure
	stabilizer      SurplusStabilizer
	cooldown        map[string]time.Time
	operationCursor uint64
	cancelCursor    uint64
	candidateCursor uint64
}

type scaleInObservation struct {
	nomadNodes       []NomadScaleInNode
	workerCandidates []ScaleInCandidateObservation
	cloud            ScaleInASGSnapshot
	observedAt       time.Time
}

func NewWithScaleIn(config *Config, demand DemandReader, snapshot CapacitySnapshotReader, nodes NodeCounter, target ScaleTarget, inventory ScaleInNodeInventory, workers ScaleInWorkerControl, infrastructure ScaleInInfrastructure, audits ...AuditSink) *Reconciler {
	reconciler := New(config, demand, snapshot, nodes, target, audits...)
	reconciler.scaleIn = &scaleInRuntime{inventory: inventory, workers: workers, infrastructure: infrastructure, cooldown: make(map[string]time.Time)}

	return reconciler
}

func (r *Reconciler) scaleOutRequiredForEnforce(ctx context.Context, now time.Time, rawRequired int64) (int64, error) {
	return r.scaleOutRequiredForEnforceAt(ctx, func() time.Time { return now }, rawRequired)
}

func (r *Reconciler) scaleOutRequiredForEnforceAt(ctx context.Context, currentTime func() time.Time, rawRequired int64) (int64, error) {
	if err := r.requireScaleInDependencies(); err != nil {
		return 0, err
	}
	observation, err := r.readScaleInObservation(ctx, currentTime)
	if err != nil {
		return 0, fmt.Errorf("read scale-in inputs before scale-out: %w", err)
	}

	return scaleOutRequiredFromObservation(rawRequired, observation), nil
}

func scaleOutRequiredFromObservation(rawRequired int64, observation scaleInObservation) int64 {
	unavailable := make(map[string]struct{})
	for _, node := range activeScaleInOperations(observation.nomadNodes) {
		instance, member := observation.cloud.Instances[node.NodeID]
		if !member {
			continue
		}
		if instance.LifecycleState == "InService" && operationSupersededByReadyWorker(observation.observedAt, node, observation.workerCandidates, observation.nomadNodes) {
			continue
		}
		unavailable[node.NodeID] = struct{}{}
	}

	// Once desired capacity is below current membership, that difference is an
	// already-committed scale-in. Do not replace owned drains that ASG is already
	// removing; only compensate drains that still occupy desired capacity.
	committedScaleIn := max(int32(0), int32(len(observation.cloud.Instances))-observation.cloud.DesiredCapacity)
	uncommittedOwnedDrains := max(int32(0), int32(len(unavailable))-committedScaleIn)

	return ScaleOutRequired(rawRequired, uncommittedOwnedDrains, ScaleInModeEnforce)
}

func (r *Reconciler) requireScaleInDependencies() error {
	if r.scaleIn == nil || r.scaleIn.inventory == nil || r.scaleIn.workers == nil || r.scaleIn.infrastructure == nil {
		return errors.New("scale-in dependencies are required in enforce mode")
	}

	return nil
}

func (r *Reconciler) reconcileScaleIn(ctx context.Context, now time.Time, workloadCount int64) (Result, error) {
	return r.reconcileScaleInAt(ctx, func() time.Time { return now }, workloadCount, Result{}, true)
}

// reconcileScaleInAt is level-triggered: every safety decision is reconstructed
// from fresh Nomad, Worker, and ASG state. Process-local fields only rate-limit
// idempotent work and are never required to recover correctness.
func (r *Reconciler) reconcileScaleInAt(ctx context.Context, currentTime func() time.Time, workloadCount int64, result Result, allowNewDrains bool) (Result, error) {
	if r.config.ScaleInMode == "" || r.config.ScaleInMode == ScaleInModeOff {
		return result, nil
	}
	if err := r.requireScaleInDependencies(); err != nil {
		return result, err
	}

	observation, err := r.readScaleInObservation(ctx, currentTime)
	if err != nil {
		r.scaleIn.stabilizer.Reset()

		return result, err
	}

	return r.reconcileScaleInObservation(ctx, currentTime, workloadCount, result, allowNewDrains, observation)
}

func (r *Reconciler) readScaleInObservation(ctx context.Context, currentTime func() time.Time) (scaleInObservation, error) {
	nomadNodes, err := r.scaleIn.inventory.Inventory(ctx, r.config.NodePool)
	if err != nil {
		return scaleInObservation{}, fmt.Errorf("read scale-in Nomad inventory: %w", err)
	}
	workerCandidates, err := r.scaleIn.workers.ListScaleInCandidates(ctx, r.config.ClusterID)
	if err != nil {
		return scaleInObservation{}, fmt.Errorf("read worker scale-in candidates: %w", err)
	}
	cloud, err := r.scaleIn.infrastructure.Snapshot(ctx, r.config.ASGName)
	if err != nil {
		return scaleInObservation{}, fmt.Errorf("read scale-in ASG snapshot: %w", err)
	}

	return scaleInObservation{
		nomadNodes:       nomadNodes,
		workerCandidates: workerCandidates,
		cloud:            cloud,
		observedAt:       currentTime(),
	}, nil
}

func (r *Reconciler) reconcileScaleInObservation(ctx context.Context, currentTime func() time.Time, workloadCount int64, result Result, allowNewDrains bool, observation scaleInObservation) (Result, error) {
	nomadNodes := observation.nomadNodes
	workerCandidates := observation.workerCandidates
	cloud := observation.cloud
	observationTime := observation.observedAt
	r.pruneScaleInCooldown(observationTime, cloud.Instances)

	nodes := mergeScaleInNodes(observationTime, nomadNodes, workerCandidates, cloud)
	plan, err := BuildScaleInPlan(workloadCount, r.config.SlotsPerNode, r.config.MinNodes, r.config.ScaleInHeadroom, nodes)
	if err != nil {
		r.scaleIn.stabilizer.Reset()

		return result, err
	}
	result.ScaleInSafeRequired = plan.SafeRequired
	result.ScaleInAccepting = plan.AcceptingNodes
	result.ScaleInExcess = plan.Excess
	result.ScaleInStable = r.scaleIn.stabilizer.Observe(observationTime, plan.Excess, true, r.config.ScaleInStableFor)
	if r.config.ScaleInMode == ScaleInModeObserve {
		return result, nil
	}
	if !cloud.NewInstancesProtectedFromScaleIn {
		return result, errors.New("enforce scale-in requires new ASG instances to be protected from scale-in")
	}

	operations := activeScaleInOperations(nomadNodes)
	operationByInstance := make(map[string]NomadScaleInNode, len(operations))
	for _, node := range operations {
		if _, duplicate := operationByInstance[node.NodeID]; duplicate {
			return result, fmt.Errorf("multiple active scale-in operations target instance %q", node.NodeID)
		}
		operationByInstance[node.NodeID] = node
	}

	for _, node := range operations {
		if _, member := cloud.Instances[node.NodeID]; member {
			continue
		}
		if err := r.completeDepartedOperation(ctx, node, *node.Operation); err != nil {
			return result, fmt.Errorf("complete departed scale-in %q: %w", node.Operation.OperationID, err)
		}
		result.ScaleInTerminated++
	}

	armed, invalidUnprotected, verifyErr := r.classifyUnprotected(ctx, cloud, operationByInstance)
	if len(invalidUnprotected) > 0 {
		if err := r.setProtection(ctx, invalidUnprotected, true); err != nil {
			return result, errors.Join(verifyErr, fmt.Errorf("protect non-armed ASG members: %w", err))
		}

		return result, verifyErr
	}
	if verifyErr != nil {
		return result, verifyErr
	}
	superseded, supersededProgress, err := r.completeSupersededOperations(ctx, observationTime, operations, workerCandidates, nomadNodes, cloud)
	if err != nil {
		return result, err
	}
	if supersededProgress {
		result.ScaleInCancelled += superseded
		// Re-read Nomad before performing any other operation; this inventory still
		// contains the marker that was just completed.
		return result, nil
	}

	memberCount := int32(len(cloud.Instances))
	safeRequired := clampInt32(plan.SafeRequired, r.config.MinNodes, r.config.MaxNodes)
	if safeRequired > cloud.DesiredCapacity {
		target := clampInt32(int64(max(memberCount, safeRequired)), r.config.MinNodes, r.config.MaxNodes)
		if target > cloud.DesiredCapacity {
			if err := r.setDesiredCapacity(ctx, ScaleAuditEvent{Mode: r.config.Mode, WorkloadCount: workloadCount, CurrentDesired: cloud.DesiredCapacity, Target: target, BatchTrigger: "scale-in-deficit-recovery"}); err != nil {
				return result, fmt.Errorf("eliminate ASG scale-in deficit: %w", err)
			}
			result.Scaled = true
			result.TargetNodes = target
		}

		return result, nil
	}

	if !asgSettled(cloud) {
		return result, nil
	}

	shrinkable := max(int32(0), cloud.DesiredCapacity-safeRequired)
	if shrinkable == 0 || !allowNewDrains {
		cancelled, cancelErr := r.restoreOperations(ctx, currentTime, operations, cloud, ScaleInGlobalBudget)
		result.ScaleInCancelled += cancelled

		return result, cancelErr
	}

	if int32(len(armed)) > shrinkable {
		extra := armed[shrinkable:]
		if err := r.setProtection(ctx, extra, true); err != nil {
			return result, fmt.Errorf("protect surplus armed workers: %w", err)
		}

		return result, nil
	}

	if len(armed) > 0 {
		reduced, err := r.lowerDesiredFromFreshState(ctx, workloadCount, armed)
		if err != nil {
			return result, err
		}
		if reduced > 0 {
			result.Scaled = true
			result.TargetNodes = cloud.DesiredCapacity - reduced
		}

		return result, nil
	}

	progressed, progressErr := r.progressOwnedDrains(ctx, currentTime, operations, cloud)
	result.ScaleInCancelled += progressed.cancelled
	if progressed.protectionWritten {
		return result, progressErr
	}
	if progressErr != nil {
		return result, progressErr
	}
	if !result.ScaleInStable || plan.Excess <= 0 {
		return result, nil
	}

	draining, cancelled, err := r.openDrains(ctx, currentTime, observationTime, nodes, nomadNodes, operations, cloud, plan)
	result.ScaleInDraining += draining
	result.ScaleInCancelled += cancelled

	return result, err
}

func (r *Reconciler) completeSupersededOperations(ctx context.Context, now time.Time, operations []NomadScaleInNode, candidates []ScaleInCandidateObservation, nomadNodes []NomadScaleInNode, cloud ScaleInASGSnapshot) (int32, bool, error) {
	var completed int32
	var progressed bool
	for _, node := range operations {
		instance, member := cloud.Instances[node.NodeID]
		if !member || !instance.ProtectedFromScaleIn || instance.LifecycleState != "InService" || !operationSupersededByReadyWorker(now, node, candidates, nomadNodes) {
			continue
		}
		operation := *node.Operation
		completedThisOperation := false
		switch {
		case operation.Stage == "restored":
			if err := r.scaleIn.inventory.CompleteRestore(ctx, node, operation); err != nil {
				return completed, progressed, fmt.Errorf("complete superseded scale-in %q: %w", operation.OperationID, err)
			}
			completed++
			progressed = true
			completedThisOperation = true
		case operation.Stage != "restoring":
			operation.Stage = "restoring"
			if err := r.scaleIn.inventory.MarkOperationStage(ctx, node, operation); err != nil {
				return completed, progressed, fmt.Errorf("persist superseded restoring stage %q: %w", operation.OperationID, err)
			}
			progressed = true
		default:
			if err := r.finishNomadRestore(ctx, node, operation); err != nil {
				return completed, progressed, fmt.Errorf("restore superseded scale-in %q: %w", operation.OperationID, err)
			}
			completed++
			progressed = true
			completedThisOperation = true
		}
		if completedThisOperation {
			r.recordScaleInTransition(node.NodeID, operation, "complete", "identity_replaced")
		}
	}

	return completed, progressed, nil
}

type drainProgress struct {
	cancelled         int32
	protectionWritten bool
}

func (r *Reconciler) progressOwnedDrains(ctx context.Context, currentTime func() time.Time, operations []NomadScaleInNode, cloud ScaleInASGSnapshot) (drainProgress, error) {
	var result drainProgress
	rotated := rotateScaleInOperations(operations, r.scaleIn.operationCursor)
	var firstErr error
	// Recovery actions are independent. Progress all of them before considering
	// a new protection write so one broken operation cannot starve a later one.
	for _, node := range rotated {
		if ctx.Err() != nil {
			break
		}
		r.scaleIn.operationCursor++
		operation := *node.Operation
		instance, member := cloud.Instances[node.NodeID]
		if !member || instance.LifecycleState != "InService" {
			continue
		}
		var err error
		switch operation.Stage {
		case "restoring", "restored":
			var completed, protectionWritten bool
			completed, protectionWritten, err = r.restoreOne(ctx, node, operation, instance)
			if completed {
				result.cancelled++
			}
			if protectionWritten {
				result.protectionWritten = true
			}
		case "nomad_marked":
			if scaleInOperationExpired(currentTime(), operation, r.config.ScaleInTimeout) {
				_, result.protectionWritten, err = r.restoreOne(ctx, node, operation, instance)
			} else if err = r.resumeNomadMarked(ctx, node, operation); err != nil {
				_, _, restoreErr := r.restoreOne(ctx, node, operation, instance)
				err = errors.Join(err, restoreErr)
			}
		case "worker_draining":
			if scaleInOperationExpired(currentTime(), operation, r.config.ScaleInTimeout) {
				_, result.protectionWritten, err = r.restoreOne(ctx, node, operation, instance)
			}
		default:
			err = fmt.Errorf("scale-in %q has unknown stage %q", operation.OperationID, operation.Stage)
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if result.protectionWritten || firstErr != nil {
		return result, firstErr
	}

	ready := make([]string, 0, min(len(rotated), int(ScaleInGlobalBudget)))
	for _, node := range rotated {
		if len(ready) >= int(ScaleInGlobalBudget) || ctx.Err() != nil {
			break
		}
		operation := *node.Operation
		instance, member := cloud.Instances[node.NodeID]
		if !member || instance.LifecycleState != "InService" || operation.Stage != "worker_draining" || scaleInOperationExpired(currentTime(), operation, r.config.ScaleInTimeout) {
			continue
		}
		state, err := r.scaleIn.workers.VerifyWorkerScaleIn(ctx, r.config.ClusterID, node.NodeID, operation.ServiceInstanceID, operation.OperationID)
		if err != nil {
			return result, fmt.Errorf("verify scale-in %q: %w", operation.OperationID, err)
		}
		if !ownedWorkerMatches(state, node.NodeID, operation.ServiceInstanceID, operation.OperationID) {
			return result, fmt.Errorf("worker scale-in %q ownership or identity changed", operation.OperationID)
		}
		if workerShutdownReady(state) {
			ready = append(ready, node.NodeID)
		}
	}
	if len(ready) > 0 {
		slices.Sort(ready)
		if err := r.setProtection(ctx, ready, false); err != nil {
			return result, fmt.Errorf("arm %d scale-in workers: %w", len(ready), err)
		}
		result.protectionWritten = true

		return result, nil
	}

	return result, nil
}

func (r *Reconciler) openDrains(ctx context.Context, currentTime func() time.Time, observationTime time.Time, nodes []ScaleInNode, nomadNodes, operations []NomadScaleInNode, cloud ScaleInASGSnapshot, plan ScaleInPlan) (int32, int32, error) {
	emptyExcess := int32(0)
	for _, node := range EligibleScaleInCandidates(nodes, observationTime, r.config.ScaleInMinimumAge) {
		if node.KnownWorkload == 0 {
			emptyExcess++
		}
	}
	budget := BuildScaleInBudget(plan.ReadyNodes, plan.Excess, min(plan.Excess, emptyExcess), plan.DisruptionUsed)
	available := min(plan.Excess, budget.AvailableGlobal)
	if available <= 0 {
		return 0, 0, nil
	}

	activeNodeIDs := make(map[string]struct{}, len(operations))
	for _, node := range operations {
		activeNodeIDs[node.NodeID] = struct{}{}
	}
	var attempts, nonEmpty, draining, cancelled int32
	for _, candidate := range rotateScaleInCandidates(EligibleScaleInCandidates(nodes, observationTime, r.config.ScaleInMinimumAge), r.scaleIn.candidateCursor) {
		if available <= 0 || attempts >= ScaleInGlobalBudget {
			break
		}
		if err := ctx.Err(); err != nil {
			return draining, cancelled, err
		}
		r.scaleIn.candidateCursor++
		instance, member := cloud.Instances[candidate.NodeID]
		if !member || !instance.ProtectedFromScaleIn || instance.HealthStatus != "Healthy" || instance.LifecycleState != "InService" {
			continue
		}
		if _, active := activeNodeIDs[candidate.NodeID]; active {
			continue
		}
		if until := r.scaleIn.cooldown[candidate.NodeID]; currentTime().Before(until) {
			continue
		}
		if candidate.KnownWorkload > 0 && nonEmpty >= budget.AllowedNonEmpty {
			continue
		}
		nomadNode, found := findNomadNode(nomadNodes, candidate.NodeID, candidate.NomadNodeID)
		if !found || hasActiveScaleInOperation(nomadNode) {
			continue
		}

		operation := NomadScaleInOperation{OperationID: uuid.NewString(), ServiceInstanceID: candidate.ServiceInstanceID, StartedAt: currentTime(), Stage: "nomad_marked"}
		attempts++
		if err := r.scaleIn.inventory.MarkDrain(ctx, nomadNode, operation); err != nil {
			// A Nomad write error is ambiguous: the drain may already be
			// durable even though its response was lost. Stop this batch and
			// reconstruct ownership from fresh inventory on the next reconcile.
			return draining, cancelled, fmt.Errorf("mark Nomad drain %q: %w", operation.OperationID, err)
		}
		r.recordScaleInTransition(candidate.NodeID, operation, "nomad_marked", "success")
		nomadNode.Draining, nomadNode.Eligible, nomadNode.Operation = true, false, &operation
		state, err := r.scaleIn.workers.BeginWorkerScaleIn(ctx, r.config.ClusterID, candidate.NodeID, candidate.ServiceInstanceID, operation.OperationID)
		if err != nil || !ownedWorkerMatches(state, candidate.NodeID, candidate.ServiceInstanceID, operation.OperationID) {
			if err == nil {
				err = errors.New("worker identity or operation ownership changed while beginning scale-in")
			}
			_, _, rollbackErr := r.restoreOne(ctx, nomadNode, operation, instance)

			return draining, cancelled, errors.Join(fmt.Errorf("begin worker drain %q: %w", operation.OperationID, err), rollbackErr)
		}
		isNonEmpty := candidate.KnownWorkload > 0 || !workerShutdownReady(state)
		if isNonEmpty && nonEmpty >= budget.AllowedNonEmpty {
			done, _, err := r.restoreOne(ctx, nomadNode, operation, instance)
			if err != nil {
				return draining, cancelled, fmt.Errorf("restore over-budget drain %q: %w", operation.OperationID, err)
			}
			if done {
				cancelled++
			}

			continue
		}
		if isNonEmpty {
			nonEmpty++
		}
		operation.Stage = "worker_draining"
		if err := r.scaleIn.inventory.MarkOperationStage(ctx, nomadNode, operation); err != nil {
			_, _, rollbackErr := r.restoreOne(ctx, nomadNode, operation, instance)

			return draining, cancelled, errors.Join(fmt.Errorf("persist worker drain stage %q: %w", operation.OperationID, err), rollbackErr)
		}
		r.recordScaleInTransition(candidate.NodeID, operation, "worker_draining", "success")
		draining++
		available--
	}

	return draining, cancelled, nil
}

func (r *Reconciler) classifyUnprotected(ctx context.Context, cloud ScaleInASGSnapshot, operations map[string]NomadScaleInNode) (armed, invalid []string, firstErr error) {
	for instanceID, instance := range cloud.Instances {
		if instance.ProtectedFromScaleIn {
			continue
		}
		node, owned := operations[instanceID]
		if !owned || node.Operation == nil || node.Operation.Stage != "worker_draining" || !node.Draining || instance.HealthStatus != "Healthy" || instance.LifecycleState != "InService" {
			if instance.LifecycleState == "InService" {
				invalid = append(invalid, instanceID)
			}

			continue
		}
		state, err := r.scaleIn.workers.VerifyWorkerScaleIn(ctx, r.config.ClusterID, instanceID, node.Operation.ServiceInstanceID, node.Operation.OperationID)
		if err != nil || !ownedWorkerMatches(state, instanceID, node.Operation.ServiceInstanceID, node.Operation.OperationID) || !workerShutdownReady(state) {
			invalid = append(invalid, instanceID)
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("verify armed worker %q: %w", instanceID, err)
			}

			continue
		}
		armed = append(armed, instanceID)
	}
	slices.Sort(armed)
	slices.Sort(invalid)

	return armed, invalid, firstErr
}

func (r *Reconciler) setProtection(ctx context.Context, instanceIDs []string, protected bool) error {
	if len(instanceIDs) == 0 {
		return nil
	}
	for start := 0; start < len(instanceIDs); start += int(ScaleInGlobalBudget) {
		end := min(start+int(ScaleInGlobalBudget), len(instanceIDs))
		if err := r.scaleIn.infrastructure.SetInstanceProtection(ctx, r.config.ASGName, instanceIDs[start:end], protected); err != nil {
			return err
		}
	}

	return nil
}

func asgSettled(cloud ScaleInASGSnapshot) bool {
	if cloud.ActiveInstanceRefresh || cloud.TerminateSuspended || int32(len(cloud.Instances)) != cloud.DesiredCapacity {
		return false
	}
	for _, instance := range cloud.Instances {
		if instance.LifecycleState != "InService" {
			return false
		}
	}

	return true
}

func (r *Reconciler) lowerDesiredFromFreshState(ctx context.Context, workloadCount int64, expectedArmed []string) (int32, error) {
	workload, err := r.snapshot.Snapshot(ctx, r.config.ClusterID)
	if err != nil {
		return 0, fmt.Errorf("refresh workload before desired reduction: %w", err)
	}
	nomadNodes, err := r.scaleIn.inventory.Inventory(ctx, r.config.NodePool)
	if err != nil {
		return 0, fmt.Errorf("refresh Nomad before desired reduction: %w", err)
	}
	cloud, err := r.scaleIn.infrastructure.Snapshot(ctx, r.config.ASGName)
	if err != nil {
		return 0, fmt.Errorf("refresh ASG before desired reduction: %w", err)
	}
	if !cloud.NewInstancesProtectedFromScaleIn || !asgSettled(cloud) {
		return 0, nil
	}

	operationByInstance := make(map[string]NomadScaleInNode)
	for _, node := range activeScaleInOperations(nomadNodes) {
		if _, duplicate := operationByInstance[node.NodeID]; duplicate {
			return 0, fmt.Errorf("multiple active scale-in operations target instance %q", node.NodeID)
		}
		operationByInstance[node.NodeID] = node
	}
	armed, invalid, verifyErr := r.classifyUnprotected(ctx, cloud, operationByInstance)
	if len(invalid) > 0 {
		if err := r.setProtection(ctx, invalid, true); err != nil {
			return 0, errors.Join(verifyErr, err)
		}

		return 0, verifyErr
	}
	if verifyErr != nil {
		return 0, verifyErr
	}
	if !sameStringSetSubset(armed, expectedArmed) {
		return 0, errors.New("fresh ASG snapshot contains an unexpected armed worker")
	}

	safeRequired := ceilDiv(workload.WorkloadCount, int64(r.config.SlotsPerNode))
	safeRequired = saturatingAdd(safeRequired, ceilPercent(safeRequired, r.config.ScaleInHeadroom))
	safeRequired = max(safeRequired, int64(r.config.MinNodes))
	safeRequired32 := clampInt32(safeRequired, r.config.MinNodes, r.config.MaxNodes)
	reduction := min(int32(len(armed)), cloud.DesiredCapacity-safeRequired32, ScaleInGlobalBudget)
	if reduction <= 0 {
		return 0, nil
	}
	target := cloud.DesiredCapacity - reduction
	if err := r.setDesiredCapacity(ctx, ScaleAuditEvent{
		Mode: r.config.Mode, WorkloadCount: workloadCount, CurrentDesired: cloud.DesiredCapacity,
		Target: target, BatchTrigger: "armed-scale-in",
	}); err != nil {
		return 0, fmt.Errorf("set scale-in desired capacity to %d: %w", target, err)
	}

	return reduction, nil
}

func sameStringSetSubset(actual, expected []string) bool {
	allowed := make(map[string]struct{}, len(expected))
	for _, value := range expected {
		allowed[value] = struct{}{}
	}
	for _, value := range actual {
		if _, ok := allowed[value]; !ok {
			return false
		}
	}

	return true
}

func (r *Reconciler) restoreOperations(ctx context.Context, currentTime func() time.Time, operations []NomadScaleInNode, cloud ScaleInASGSnapshot, limit int32) (int32, error) {
	var cancelled, attempted int32
	var firstErr error
	for _, node := range rotateScaleInOperations(operations, r.scaleIn.cancelCursor) {
		if attempted >= limit || ctx.Err() != nil {
			break
		}
		instance, member := cloud.Instances[node.NodeID]
		if !member || instance.LifecycleState != "InService" {
			continue
		}
		attempted++
		r.scaleIn.cancelCursor++
		done, _, err := r.restoreOne(ctx, node, *node.Operation, instance)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("restore scale-in %q: %w", node.Operation.OperationID, err)
			}

			continue
		}
		if done {
			r.recordScaleInCooldown(currentTime(), node, *node.Operation)
			cancelled++
		}
	}

	return cancelled, firstErr
}

// restoreOne enforces the safe recovery order: ASG protection, Worker
// admission, then Nomad eligibility. A protection write is confirmed only by a
// later reconcile snapshot, so this call returns immediately after that write.
func (r *Reconciler) restoreOne(ctx context.Context, node NomadScaleInNode, operation NomadScaleInOperation, instance ScaleInASGInstance) (completed, protectionWritten bool, err error) {
	if !instance.ProtectedFromScaleIn {
		if err := r.setProtection(ctx, []string{node.NodeID}, true); err != nil {
			return false, false, err
		}

		return false, true, nil
	}
	if instance.LifecycleState != "InService" {
		return false, false, nil
	}
	if operation.Stage == "restored" {
		return true, false, r.scaleIn.inventory.CompleteRestore(ctx, node, operation)
	}
	if operation.Stage != "restoring" {
		state, err := r.scaleIn.workers.VerifyWorkerScaleIn(ctx, r.config.ClusterID, node.NodeID, operation.ServiceInstanceID, operation.OperationID)
		if err != nil {
			return false, false, fmt.Errorf("verify worker before restoring: %w", err)
		}
		if !ownedWorkerMatches(state, node.NodeID, operation.ServiceInstanceID, operation.OperationID) {
			return false, false, errors.New("worker ownership changed before restoring")
		}
		operation.Stage = "restoring"
		if err := r.scaleIn.inventory.MarkOperationStage(ctx, node, operation); err != nil {
			return false, false, fmt.Errorf("persist restoring stage: %w", err)
		}
		r.recordScaleInTransition(node.NodeID, operation, "restoring", "started")

		return false, false, nil
	}
	state, err := r.scaleIn.workers.CancelWorkerScaleIn(ctx, r.config.ClusterID, node.NodeID, operation.ServiceInstanceID, operation.OperationID)
	if err != nil {
		return false, false, fmt.Errorf("cancel worker scale-in: %w", err)
	}
	if state.NodeID != node.NodeID || state.ServiceInstanceID != operation.ServiceInstanceID || !state.ScaleInProtocolSupport || state.ServiceStatus != "Healthy" {
		return false, false, errors.New("worker scale-in cancel did not restore the expected protocol-capable Healthy service instance")
	}
	if err := r.finishNomadRestore(ctx, node, operation); err != nil {
		return false, false, err
	}
	r.recordScaleInTransition(node.NodeID, operation, "complete", "cancelled")

	return true, false, nil
}

func (r *Reconciler) finishNomadRestore(ctx context.Context, node NomadScaleInNode, operation NomadScaleInOperation) error {
	if node.Draining || !node.Eligible {
		if err := r.scaleIn.inventory.RestoreDrain(ctx, node, operation); err != nil {
			return err
		}
	}

	return r.scaleIn.inventory.CompleteRestore(ctx, node, operation)
}

func (r *Reconciler) completeDepartedOperation(ctx context.Context, node NomadScaleInNode, operation NomadScaleInOperation) error {
	if (operation.Stage == "restoring" || operation.Stage == "restored") && !node.Draining && node.Eligible {
		return r.scaleIn.inventory.CompleteRestore(ctx, node, operation)
	}

	return r.scaleIn.inventory.CompleteTermination(ctx, node, operation)
}

func (r *Reconciler) resumeNomadMarked(ctx context.Context, node NomadScaleInNode, operation NomadScaleInOperation) error {
	state, err := r.scaleIn.workers.BeginWorkerScaleIn(ctx, r.config.ClusterID, node.NodeID, operation.ServiceInstanceID, operation.OperationID)
	if err != nil {
		return err
	}
	if !ownedWorkerMatches(state, node.NodeID, operation.ServiceInstanceID, operation.OperationID) {
		return errors.New("worker identity or operation ownership changed while resuming Nomad-marked scale-in")
	}
	operation.Stage = "worker_draining"
	if err := r.scaleIn.inventory.MarkOperationStage(ctx, node, operation); err != nil {
		return err
	}
	r.recordScaleInTransition(node.NodeID, operation, "worker_draining", "recovered")

	return nil
}

func (r *Reconciler) pruneScaleInCooldown(now time.Time, instances map[string]ScaleInASGInstance) {
	for nodeID, until := range r.scaleIn.cooldown {
		if _, member := instances[nodeID]; !member || !now.Before(until) {
			delete(r.scaleIn.cooldown, nodeID)
		}
	}
}

func scaleInOperationExpired(now time.Time, operation NomadScaleInOperation, timeout time.Duration) bool {
	return !operation.StartedAt.IsZero() && !now.Before(operation.StartedAt.Add(timeout))
}

func (r *Reconciler) recordScaleInCooldown(now time.Time, node NomadScaleInNode, operation NomadScaleInOperation) {
	if scaleInOperationExpired(now, operation, r.config.ScaleInTimeout) {
		r.scaleIn.cooldown[node.NodeID] = now.Add(r.config.ScaleInTimeout)
	}
}

func (r *Reconciler) recordScaleInTransition(nodeID string, operation NomadScaleInOperation, stage, outcome string) {
	r.recordAudit(ScaleAuditEvent{
		Event: AuditEventScaleInTransition, ControllerInstanceID: r.controllerInstanceID,
		Mode: r.config.Mode, Outcome: outcome, ScaleInOperationID: operation.OperationID,
		ScaleInNodeID: nodeID, ScaleInStage: stage,
	})
}

func activeScaleInOperations(nodes []NomadScaleInNode) []NomadScaleInNode {
	result := make([]NomadScaleInNode, 0)
	for _, node := range nodes {
		if hasActiveScaleInOperation(node) {
			result = append(result, node)
		}
	}

	return result
}

func hasActiveScaleInOperation(node NomadScaleInNode) bool {
	if node.Operation == nil || node.Operation.Stage == "complete" {
		return false
	}

	return node.Operation.Stage != "restored" || !node.Eligible || node.Draining
}

func mergeScaleInNodes(now time.Time, nomadNodes []NomadScaleInNode, candidates []ScaleInCandidateObservation, cloud ScaleInASGSnapshot) []ScaleInNode {
	byNodeID := make(map[string]ScaleInCandidateObservation, len(candidates))
	for _, candidate := range candidates {
		if candidateObservationFresh(now, candidate.ObservedAt) {
			byNodeID[candidate.NodeID] = candidate
		}
	}
	nomadByID := make(map[string]NomadScaleInNode, len(nomadNodes))
	for _, nomad := range nomadNodes {
		if nomad.NomadNodeID != "" {
			nomadByID[nomad.NomadNodeID] = nomad
		}
	}
	result := make([]ScaleInNode, 0, len(cloud.Instances))
	for instanceID, instance := range cloud.Instances {
		candidate, observed := byNodeID[instanceID]
		nomad, found := nomadByID[candidate.NomadNodeID]
		if !observed || candidate.NomadNodeID == "" || !found || nomad.NodeID != instanceID {
			result = append(result, ScaleInNode{NodeID: instanceID, LaunchTime: instance.LaunchTime, Terminating: instance.LifecycleState != "InService"})

			continue
		}
		result = append(result, ScaleInNode{
			NodeID: instanceID, NomadNodeID: nomad.NomadNodeID, ServiceInstanceID: candidate.ServiceInstanceID,
			RunningSandboxes: candidate.RunningSandboxes, KnownWorkload: candidate.KnownWorkload,
			NomadCreateIndex: nomad.CreateIndex, LaunchTime: instance.LaunchTime,
			ScaleInProtocolSupport: candidate.ScaleInProtocolSupport, Ready: nomad.Ready,
			Healthy:  candidate.ServiceStatus == "ready" && instance.HealthStatus == "Healthy",
			Eligible: nomad.Eligible, Draining: nomad.Draining, Terminating: instance.LifecycleState != "InService",
		})
	}
	slices.SortFunc(result, func(left, right ScaleInNode) int { return compareStrings(left.NodeID, right.NodeID) })

	return result
}

func operationSupersededByReadyWorker(now time.Time, node NomadScaleInNode, candidates []ScaleInCandidateObservation, nomadNodes []NomadScaleInNode) bool {
	for _, candidate := range candidates {
		if candidate.NodeID != node.NodeID || !candidateObservationFresh(now, candidate.ObservedAt) || candidate.ServiceStatus != "ready" || !candidate.ScaleInProtocolSupport || candidate.ServiceInstanceID == "" || candidate.ServiceInstanceID == node.Operation.ServiceInstanceID {
			continue
		}
		if candidate.NomadNodeID == "" {
			return false
		}
		replacement, found := findNomadNode(nomadNodes, node.NodeID, candidate.NomadNodeID)
		if candidate.NomadNodeID == node.NomadNodeID {
			// The Worker process can restart without replacing its Nomad client.
			// A fresh ready service identity proves the old Worker operation no
			// longer exists, so only the owned Nomad marker needs restoration.
			return found && replacement.Ready
		}

		return found && replacement.Ready && replacement.Eligible && !replacement.Draining
	}

	return false
}

func candidateObservationFresh(now, observedAt time.Time) bool {
	return !observedAt.IsZero() && !observedAt.After(now) && now.Sub(observedAt) <= scaleInCandidateMaxAge
}

func findNomadNode(nodes []NomadScaleInNode, nodeID, nomadNodeID string) (NomadScaleInNode, bool) {
	if nomadNodeID == "" {
		return NomadScaleInNode{}, false
	}
	for _, node := range nodes {
		if node.NodeID == nodeID && node.NomadNodeID == nomadNodeID {
			return node, true
		}
	}

	return NomadScaleInNode{}, false
}

func rotateScaleInOperations(nodes []NomadScaleInNode, cursor uint64) []NomadScaleInNode {
	if len(nodes) < 2 {
		return nodes
	}
	start := int(cursor % uint64(len(nodes)))
	rotated := make([]NomadScaleInNode, 0, len(nodes))
	rotated = append(rotated, nodes[start:]...)
	rotated = append(rotated, nodes[:start]...)

	return rotated
}

func rotateScaleInCandidates(nodes []ScaleInNode, cursor uint64) []ScaleInNode {
	if len(nodes) < 2 {
		return nodes
	}
	start := int(cursor % uint64(len(nodes)))
	rotated := make([]ScaleInNode, 0, len(nodes))
	rotated = append(rotated, nodes[start:]...)
	rotated = append(rotated, nodes[:start]...)

	return rotated
}

func ownedWorkerMatches(state WorkerScaleInState, nodeID, serviceID, operationID string) bool {
	return state.NodeID == nodeID && state.ServiceInstanceID == serviceID && state.ScaleInProtocolSupport && state.ServiceStatus == "Draining" && state.ScaleInOperationID == operationID
}

func workerShutdownReady(state WorkerScaleInState) bool {
	return state.ShutdownReady && state.SandboxListEmpty && state.RunningSandboxes == 0 && state.StartsInFlight == 0 && state.LifecycleCleanupsInFlight == 0 && state.SnapshotUploadsInFlight == 0
}

func compareStrings(left, right string) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}

	return 0
}
