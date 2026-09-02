package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	maxScaleInProgressPerReconcile = 10
	// The API refreshes its worker cache every 20 seconds. Two complete cache
	// cycles tolerate one delayed sync without authorizing stale placement data.
	scaleInCandidateMaxAge = 40 * time.Second
)

type scaleInRuntime struct {
	inventory      ScaleInNodeInventory
	workers        ScaleInWorkerControl
	infrastructure ScaleInInfrastructure
	stabilizer     SurplusStabilizer
	cooldown       map[string]time.Time
}

func NewWithScaleIn(
	config *Config,
	demand DemandReader,
	snapshot CapacitySnapshotReader,
	nodes NodeCounter,
	target ScaleTarget,
	inventory ScaleInNodeInventory,
	workers ScaleInWorkerControl,
	infrastructure ScaleInInfrastructure,
	audits ...AuditSink,
) *Reconciler {
	reconciler := New(config, demand, snapshot, nodes, target, audits...)
	reconciler.scaleIn = &scaleInRuntime{
		inventory:      inventory,
		workers:        workers,
		infrastructure: infrastructure,
		cooldown:       make(map[string]time.Time),
	}

	return reconciler
}

func (r *Reconciler) scaleOutRequiredForEnforce(ctx context.Context, now time.Time, workloadCount, rawRequired int64) (int64, error) {
	if r.scaleIn == nil || r.scaleIn.inventory == nil || r.scaleIn.workers == nil || r.scaleIn.infrastructure == nil {
		return 0, errors.New("scale-in dependencies are required in enforce mode")
	}
	nomadNodes, err := r.scaleIn.inventory.Inventory(ctx, r.config.NodePool)
	if err != nil {
		return 0, fmt.Errorf("read owned drains before scale-out: %w", err)
	}
	workers, err := r.scaleIn.workers.ListScaleInCandidates(ctx, r.config.ClusterID)
	if err != nil {
		return 0, fmt.Errorf("read workers before scale-out: %w", err)
	}
	cloud, err := r.scaleIn.infrastructure.Snapshot(ctx, r.config.ASGName)
	if err != nil {
		return 0, fmt.Errorf("read ASG before scale-out: %w", err)
	}
	plan, err := BuildScaleInPlan(workloadCount, r.config.SlotsPerNode, r.config.MinNodes, r.config.ScaleInHeadroom, mergeScaleInNodes(now, nomadNodes, workers, cloud))
	if err != nil {
		return 0, err
	}
	if int64(plan.AcceptingNodes) < plan.SafeRequired {
		deficit := plan.SafeRequired - int64(plan.AcceptingNodes)
		// Scale-out must continue even if restoration is incomplete. The re-read
		// below counts any still-owned drain as unavailable capacity, while
		// cancelUncommitted records and returns the restoration failure.
		_, _ = r.cancelUncommitted(ctx, nomadNodes, int32(min(deficit, int64(maxScaleInProgressPerReconcile))))
		nomadNodes, err = r.scaleIn.inventory.Inventory(ctx, r.config.NodePool)
		if err != nil {
			return 0, fmt.Errorf("re-read owned drains after capacity restore: %w", err)
		}
	}

	var unavailableOwned int32
	for _, node := range activeScaleInOperations(nomadNodes) {
		if node.Operation.Stage != "terminating" {
			unavailableOwned++

			continue
		}
		// A terminating worker that is still an ASG member remains unavailable
		// regardless of whether AWS accepted the exact terminate request. Count it
		// until membership proves that desired capacity actually left the group.
		if _, found := cloud.Instances[node.NodeID]; found && node.Operation.ActivityID == "" {
			unavailableOwned++
		}
	}

	return ScaleOutRequired(rawRequired, unavailableOwned, ScaleInModeEnforce), nil
}

func (r *Reconciler) reconcileScaleIn(ctx context.Context, now time.Time, workloadCount int64, result Result) (Result, error) {
	if r.config.ScaleInMode == "" || r.config.ScaleInMode == ScaleInModeOff {
		return result, nil
	}
	if r.scaleIn == nil || r.scaleIn.inventory == nil || r.scaleIn.workers == nil || r.scaleIn.infrastructure == nil {
		return result, errors.New("scale-in dependencies are required when scale-in mode is not off")
	}

	nomadNodes, err := r.scaleIn.inventory.Inventory(ctx, r.config.NodePool)
	if err != nil {
		r.scaleIn.stabilizer.Reset()

		return result, fmt.Errorf("read scale-in Nomad inventory: %w", err)
	}
	workerCandidates, err := r.scaleIn.workers.ListScaleInCandidates(ctx, r.config.ClusterID)
	if err != nil {
		r.scaleIn.stabilizer.Reset()
		if r.config.ScaleInMode == ScaleInModeEnforce {
			_, cancelErr := r.cancelUncommitted(ctx, nomadNodes, maxScaleInProgressPerReconcile)
			err = errors.Join(err, cancelErr)
		}

		return result, fmt.Errorf("read worker scale-in candidates: %w", err)
	}
	cloud, err := r.scaleIn.infrastructure.Snapshot(ctx, r.config.ASGName)
	if err != nil {
		r.scaleIn.stabilizer.Reset()
		if r.config.ScaleInMode == ScaleInModeEnforce {
			_, cancelErr := r.cancelUncommitted(ctx, nomadNodes, maxScaleInProgressPerReconcile)
			err = errors.Join(err, cancelErr)
		}

		return result, fmt.Errorf("read scale-in ASG snapshot: %w", err)
	}

	nodes := mergeScaleInNodes(now, nomadNodes, workerCandidates, cloud)
	plan, err := BuildScaleInPlan(workloadCount, r.config.SlotsPerNode, r.config.MinNodes, r.config.ScaleInHeadroom, nodes)
	if err != nil {
		r.scaleIn.stabilizer.Reset()

		return result, err
	}
	result.ScaleInSafeRequired = plan.SafeRequired
	result.ScaleInAccepting = plan.AcceptingNodes
	result.ScaleInExcess = plan.Excess
	stable := r.scaleIn.stabilizer.Observe(now, plan.Excess, true, r.config.ScaleInStableFor)
	result.ScaleInStable = stable
	if r.config.ScaleInMode == ScaleInModeObserve {
		return result, nil
	}

	operations := activeScaleInOperations(nomadNodes)
	if plan.AcceptingNodes < int32(min(int64(^uint32(0)>>1), plan.SafeRequired)) {
		deficit := int32(min(int64(^uint32(0)>>1), plan.SafeRequired)) - plan.AcceptingNodes
		cancelled, cancelErr := r.cancelUncommitted(ctx, operations, min(deficit, maxScaleInProgressPerReconcile))
		result.ScaleInCancelled += cancelled

		return result, cancelErr
	}

	progressed := 0
	var firstOperationErr error
	for _, node := range operations {
		if progressed >= int(ScaleInGlobalBudget) {
			break
		}
		progressed++
		operation := *node.Operation
		switch operation.Stage {
		case "restoring":
			if err := r.finishCancel(ctx, node, operation); err == nil {
				result.ScaleInCancelled++
			} else if firstOperationErr == nil {
				firstOperationErr = fmt.Errorf("finish restoring scale-in %q: %w", operation.OperationID, err)
			}
		case "restored":
			if err := r.scaleIn.inventory.CompleteRestore(ctx, node, operation); err == nil {
				result.ScaleInCancelled++
				r.recordScaleInTransition(node.NodeID, operation, "complete", "cancelled", "")
			} else if firstOperationErr == nil {
				firstOperationErr = fmt.Errorf("finish restored scale-in %q: %w", operation.OperationID, err)
			}
		case "terminating":
			if err := r.reconcileCommitted(ctx, node, operation, cloud); err != nil && firstOperationErr == nil {
				firstOperationErr = fmt.Errorf("reconcile committed scale-in %q: %w", operation.OperationID, err)
			}
		case "nomad_marked":
			if err := r.resumeNomadMarked(ctx, node, operation); err != nil && firstOperationErr == nil {
				firstOperationErr = fmt.Errorf("resume Nomad-marked scale-in %q: %w", operation.OperationID, err)
			}
		case "worker_draining":
			if !now.Before(operation.StartedAt.Add(r.config.ScaleInTimeout)) {
				if err := r.cancelOperation(ctx, node, operation); err == nil {
					r.scaleIn.cooldown[node.NodeID] = now.Add(r.config.ScaleInTimeout)
					result.ScaleInCancelled++
				} else if firstOperationErr == nil {
					firstOperationErr = fmt.Errorf("cancel expired scale-in %q: %w", operation.OperationID, err)
				}

				continue
			}
			state, verifyErr := r.scaleIn.workers.VerifyWorkerScaleIn(ctx, r.config.ClusterID, node.NodeID, operation.ServiceInstanceID)
			if verifyErr != nil {
				if cancelErr := r.cancelOperation(ctx, node, operation); cancelErr != nil && firstOperationErr == nil {
					firstOperationErr = errors.Join(
						fmt.Errorf("verify scale-in %q: %w", operation.OperationID, verifyErr),
						fmt.Errorf("restore scale-in %q: %w", operation.OperationID, cancelErr),
					)
				} else if firstOperationErr == nil {
					firstOperationErr = fmt.Errorf("verify scale-in %q: %w", operation.OperationID, verifyErr)
				}

				continue
			}
			if !workerIdentityMatches(state, node.NodeID, operation.ServiceInstanceID) {
				if err := r.cancelOperation(ctx, node, operation); err == nil {
					result.ScaleInCancelled++
				} else if firstOperationErr == nil {
					firstOperationErr = fmt.Errorf("cancel identity-changed scale-in %q: %w", operation.OperationID, err)
				}

				continue
			}
			if workerShutdownReady(state) {
				accepted, err := r.commitTermination(ctx, now, node, operation)
				if err != nil {
					if firstOperationErr == nil {
						firstOperationErr = fmt.Errorf("commit scale-in %q: %w", operation.OperationID, err)
					}
				} else if accepted {
					result.ScaleInTerminated++
				}
			}
		default:
			if firstOperationErr == nil {
				firstOperationErr = fmt.Errorf("scale-in %q has unknown stage %q", operation.OperationID, operation.Stage)
			}
		}
	}
	if !stable || plan.Excess <= 0 {
		return result, firstOperationErr
	}

	emptyExcess := plan.Excess
	budget := BuildScaleInBudget(plan.ReadyNodes, plan.Excess, emptyExcess, plan.DisruptionUsed)
	// Existing operations consume the global disruption window through
	// budget.AvailableGlobal. The per-reconcile limit only rate-limits newly
	// opened drains; subtracting already-progressed operations here would cap the
	// rolling window at ten instead of allowing it to fill toward fifty.
	allowed := min(plan.Excess, budget.AvailableGlobal, int32(maxScaleInProgressPerReconcile))
	if allowed <= 0 {
		return result, firstOperationErr
	}

	var newNonEmpty int32
	for _, candidate := range EligibleScaleInCandidates(nodes, now, r.config.ScaleInMinimumAge) {
		if allowed <= 0 {
			break
		}
		// Avoid disrupting a worker that the same authoritative ASG snapshot
		// already proves cannot be terminated. The final check in
		// commitTermination remains necessary to close the race with changes that
		// happen after the drain starts.
		if scaleInTerminationBlock(cloud, candidate.NodeID) != "" {
			continue
		}
		if until := r.scaleIn.cooldown[candidate.NodeID]; now.Before(until) {
			continue
		}
		nomadNode, found := findNomadNode(nomadNodes, candidate.NodeID)
		if !found || hasActiveScaleInOperation(nomadNode) {
			continue
		}
		operation := NomadScaleInOperation{
			OperationID:       uuid.NewString(),
			ServiceInstanceID: candidate.ServiceInstanceID,
			StartedAt:         now,
			Stage:             "nomad_marked",
		}
		if err := r.scaleIn.inventory.MarkDrain(ctx, nomadNode, operation); err != nil {
			continue
		}
		r.recordScaleInTransition(candidate.NodeID, operation, "nomad_marked", "success", "")
		nomadNode.Draining = true
		nomadNode.Eligible = false
		nomadNode.Operation = &operation
		state, err := r.scaleIn.workers.BeginWorkerScaleIn(ctx, r.config.ClusterID, candidate.NodeID, candidate.ServiceInstanceID)
		if err != nil || !workerIdentityMatches(state, candidate.NodeID, candidate.ServiceInstanceID) {
			beginErr := err
			if beginErr == nil {
				beginErr = errors.New("worker identity changed while beginning scale-in")
			}
			if cancelErr := r.cancelOperation(ctx, nomadNode, operation); cancelErr != nil {
				return result, errors.Join(
					fmt.Errorf("begin worker drain for %q: %w", operation.OperationID, beginErr),
					fmt.Errorf("rollback failed for worker drain %q: %w", operation.OperationID, cancelErr),
				)
			}

			continue
		}
		if !workerShutdownReady(state) && newNonEmpty >= budget.AllowedNonEmpty {
			if cancelErr := r.cancelOperation(ctx, nomadNode, operation); cancelErr != nil {
				return result, fmt.Errorf("rollback over-budget drain %q: %w", operation.OperationID, cancelErr)
			}

			continue
		}
		if !workerShutdownReady(state) {
			newNonEmpty++
		}
		operation.Stage = "worker_draining"
		if err := r.scaleIn.inventory.MarkOperationStage(ctx, nomadNode, operation); err != nil {
			r.recordScaleInTransition(candidate.NodeID, operation, "worker_draining", "failed", err.Error())
			if cancelErr := r.cancelOperation(ctx, nomadNode, operation); cancelErr != nil {
				return result, errors.Join(
					fmt.Errorf("persist worker drain stage for %q: %w", operation.OperationID, err),
					fmt.Errorf("rollback failed for worker drain %q: %w", operation.OperationID, cancelErr),
				)
			}
			if firstOperationErr == nil {
				firstOperationErr = fmt.Errorf("persist worker drain stage for %q: %w", operation.OperationID, err)
			}

			continue
		}
		r.recordScaleInTransition(candidate.NodeID, operation, "worker_draining", "success", "")
		result.ScaleInDraining++
		allowed--
	}

	return result, firstOperationErr
}

func (r *Reconciler) resumeNomadMarked(ctx context.Context, node NomadScaleInNode, operation NomadScaleInOperation) error {
	state, err := r.scaleIn.workers.BeginWorkerScaleIn(ctx, r.config.ClusterID, node.NodeID, operation.ServiceInstanceID)
	if err != nil || !workerIdentityMatches(state, node.NodeID, operation.ServiceInstanceID) {
		cause := err
		if cause == nil {
			cause = errors.New("worker identity changed while resuming Nomad-marked scale-in")
		}
		if cancelErr := r.cancelOperation(ctx, node, operation); cancelErr != nil {
			return errors.Join(cause, fmt.Errorf("cancel scale-in: %w", cancelErr))
		}

		return cause
	}
	operation.Stage = "worker_draining"
	if err := r.scaleIn.inventory.MarkOperationStage(ctx, node, operation); err != nil {
		if cancelErr := r.cancelOperation(ctx, node, operation); cancelErr != nil {
			return errors.Join(
				fmt.Errorf("persist worker drain stage: %w", err),
				fmt.Errorf("cancel scale-in: %w", cancelErr),
			)
		}

		return fmt.Errorf("persist worker drain stage: %w", err)
	}
	r.recordScaleInTransition(node.NodeID, operation, "worker_draining", "recovered", "")

	return nil
}

func (r *Reconciler) commitTermination(ctx context.Context, now time.Time, node NomadScaleInNode, operation NomadScaleInOperation) (bool, error) {
	worker, err := r.scaleIn.workers.VerifyWorkerScaleIn(ctx, r.config.ClusterID, node.NodeID, operation.ServiceInstanceID)
	if err != nil || !workerIdentityMatches(worker, node.NodeID, operation.ServiceInstanceID) || !workerShutdownReady(worker) {
		return false, r.cancelBeforeTermination(ctx, node, operation, errors.New("final worker scale-in verification failed"))
	}
	workload, err := r.snapshot.Snapshot(ctx, r.config.ClusterID)
	if err != nil {
		return false, r.cancelBeforeTermination(ctx, node, operation, fmt.Errorf("refresh workload before termination: %w", err))
	}
	nomadNodes, err := r.scaleIn.inventory.Inventory(ctx, r.config.NodePool)
	if err != nil {
		return false, r.cancelBeforeTermination(ctx, node, operation, fmt.Errorf("refresh Nomad before termination: %w", err))
	}
	freshNode, found := findNomadNode(nomadNodes, node.NodeID)
	if !found || freshNode.Operation == nil || freshNode.Operation.OperationID != operation.OperationID || !freshNode.Draining {
		return false, r.cancelBeforeTermination(ctx, node, operation, errors.New("nomad scale-in ownership changed before termination"))
	}
	candidates, err := r.scaleIn.workers.ListScaleInCandidates(ctx, r.config.ClusterID)
	if err != nil {
		return false, r.cancelBeforeTermination(ctx, freshNode, operation, fmt.Errorf("refresh workers before termination: %w", err))
	}
	cloud, err := r.scaleIn.infrastructure.Snapshot(ctx, r.config.ASGName)
	if err != nil {
		return false, r.cancelBeforeTermination(ctx, freshNode, operation, fmt.Errorf("refresh ASG before termination: %w", err))
	}
	plan, err := BuildScaleInPlan(workload.WorkloadCount, r.config.SlotsPerNode, r.config.MinNodes, r.config.ScaleInHeadroom, mergeScaleInNodes(now, nomadNodes, candidates, cloud))
	if err != nil || int64(plan.AcceptingNodes) < plan.SafeRequired {
		return false, r.cancelBeforeTermination(ctx, freshNode, operation, errors.New("accepting capacity is below safe scale-in requirement"))
	}
	if block := scaleInTerminationBlock(cloud, node.NodeID); block != "" {
		return false, r.cancelBeforeTermination(ctx, freshNode, operation, fmt.Errorf("ASG blocked scale-in: %s", block))
	}

	operation.Stage = "terminating"
	if err := r.scaleIn.inventory.MarkOperationStage(ctx, freshNode, operation); err != nil {
		return false, err
	}
	r.recordScaleInTransition(node.NodeID, operation, "terminating", "started", "")
	receipt, err := r.scaleIn.infrastructure.TerminateInstance(ctx, node.NodeID)
	if err != nil {
		var terminationErr *ScaleInTerminationError
		if !errors.As(err, &terminationErr) {
			// An implementation that does not classify a destructive-call error
			// cannot prove rejection. Preserve the committed marker and never retry.
			return false, fmt.Errorf("unclassified exact termination outcome: %w", err)
		}
		if terminationErr.Outcome == ScaleInTerminationAmbiguous {
			// The request may have reached AWS. Keep the owned terminating marker;
			// later reconciles use only membership/activity and never repeat it.
			r.recordScaleInTransition(node.NodeID, operation, "terminating", "ambiguous", terminationErr.Error())

			return false, nil
		}
		if terminationErr.Outcome == ScaleInTerminationRejected {
			return false, r.cancelBeforeTermination(ctx, freshNode, operation, terminationErr)
		}

		return false, fmt.Errorf("unknown exact termination outcome %q: %w", terminationErr.Outcome, err)
	}
	operation.ActivityID = receipt.ActivityID
	if err := r.scaleIn.inventory.MarkOperationStage(ctx, freshNode, operation); err != nil {
		return false, fmt.Errorf("persist accepted AWS activity %q for scale-in %q: %w", receipt.ActivityID, operation.OperationID, err)
	}
	r.recordScaleInTransition(node.NodeID, operation, "terminating", "accepted", "")

	return true, nil
}

func (r *Reconciler) cancelBeforeTermination(ctx context.Context, node NomadScaleInNode, operation NomadScaleInOperation, cause error) error {
	if err := r.cancelOperation(ctx, node, operation); err != nil {
		return errors.Join(cause, fmt.Errorf("cancel scale-in: %w", err))
	}

	return cause
}

func (r *Reconciler) reconcileCommitted(ctx context.Context, node NomadScaleInNode, operation NomadScaleInOperation, cloud ScaleInASGSnapshot) error {
	if _, found := cloud.Instances[node.NodeID]; !found {
		if err := r.scaleIn.inventory.CompleteTermination(ctx, node, operation); err != nil {
			return err
		}
		r.recordScaleInTransition(node.NodeID, operation, "complete", "success", "")

		return nil
	}
	if operation.ActivityID == "" {
		return errors.New("exact termination outcome remains ambiguous: instance is still in ASG and no activity ID was recorded")
	}
	activity, err := r.scaleIn.infrastructure.Activity(ctx, r.config.ASGName, operation.ActivityID)
	if err != nil {
		return err
	}
	if activity == nil {
		return fmt.Errorf("AWS activity %q is not observable while instance remains in ASG", operation.ActivityID)
	}
	if activity.Status == "Failed" || activity.Status == "Cancelled" {
		return r.cancelOperation(ctx, node, operation)
	}
	if !scaleInActivityPending(activity.Status) {
		return fmt.Errorf("AWS activity %q has unknown status %q", operation.ActivityID, activity.Status)
	}

	return nil
}

func scaleInActivityPending(status string) bool {
	switch status {
	case "PendingSpotBidPlacement", "WaitingForSpotInstanceRequestId", "WaitingForSpotInstanceId", "WaitingForInstanceId", "PreInService", "InProgress", "WaitingForELBConnectionDraining", "MidLifecycleAction", "WaitingForInstanceWarmup", "Successful", "WaitingForConnectionDraining", "WaitingForInPlaceUpdateToStart", "WaitingForInPlaceUpdateToFinalize", "InPlaceUpdateInProgress":
		return true
	default:
		return false
	}
}

func (r *Reconciler) cancelUncommitted(ctx context.Context, nodes []NomadScaleInNode, limit int32) (int32, error) {
	var cancelled int32
	var firstErr error
	for _, node := range activeScaleInOperations(nodes) {
		if cancelled >= limit || node.Operation.Stage == "terminating" {
			continue
		}
		if err := r.cancelOperation(ctx, node, *node.Operation); err == nil {
			cancelled++
		} else if firstErr == nil {
			firstErr = fmt.Errorf("cancel scale-in %q: %w", node.Operation.OperationID, err)
		}
	}

	return cancelled, firstErr
}

func (r *Reconciler) cancelOperation(ctx context.Context, node NomadScaleInNode, operation NomadScaleInOperation) error {
	if operation.Stage != "restoring" {
		if err := r.scaleIn.inventory.RestoreDrain(ctx, node, operation); err != nil {
			r.recordScaleInTransition(node.NodeID, operation, "restoring", "failed", err.Error())

			return err
		}
		operation.Stage = "restoring"
		r.recordScaleInTransition(node.NodeID, operation, "restoring", "started", "")
	}
	if err := r.finishCancel(ctx, node, operation); err != nil {
		r.recordScaleInTransition(node.NodeID, operation, "restoring", "failed", err.Error())

		return err
	}
	r.recordScaleInTransition(node.NodeID, operation, "complete", "cancelled", "")

	return nil
}

func (r *Reconciler) recordScaleInTransition(nodeID string, operation NomadScaleInOperation, stage, outcome, reason string) {
	r.recordAudit(ScaleAuditEvent{
		Event:                AuditEventScaleInTransition,
		ControllerInstanceID: r.controllerInstanceID,
		Mode:                 r.config.Mode,
		Outcome:              outcome,
		ScaleInOperationID:   operation.OperationID,
		ScaleInNodeID:        nodeID,
		ScaleInStage:         stage,
		ScaleInReason:        reason,
		ASGActivityID:        operation.ActivityID,
	})
}

func (r *Reconciler) finishCancel(ctx context.Context, node NomadScaleInNode, operation NomadScaleInOperation) error {
	state, err := r.scaleIn.workers.CancelWorkerScaleIn(ctx, r.config.ClusterID, node.NodeID, operation.ServiceInstanceID)
	if err != nil {
		return fmt.Errorf("cancel worker scale-in: %w", err)
	}
	if state.NodeID != node.NodeID || state.ServiceInstanceID == "" || !state.ScaleInProtocolSupport || state.ServiceStatus != "Healthy" {
		return errors.New("worker scale-in cancel did not restore the expected protocol-capable Healthy service instance")
	}

	return r.scaleIn.inventory.CompleteRestore(ctx, node, operation)
}

func activeScaleInOperations(nodes []NomadScaleInNode) []NomadScaleInNode {
	result := make([]NomadScaleInNode, 0)
	for _, node := range nodes {
		if !hasActiveScaleInOperation(node) {
			continue
		}
		result = append(result, node)
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
	result := make([]ScaleInNode, 0, max(len(nomadNodes), len(cloud.Instances)))
	seen := make(map[string]struct{}, len(nomadNodes))
	for _, nomad := range nomadNodes {
		candidate, observed := byNodeID[nomad.NodeID]
		instance, member := cloud.Instances[nomad.NodeID]
		if !member {
			continue
		}
		result = append(result, ScaleInNode{
			NodeID:                 nomad.NodeID,
			NomadNodeID:            nomad.NomadNodeID,
			ServiceInstanceID:      candidate.ServiceInstanceID,
			RunningSandboxes:       candidate.RunningSandboxes,
			NomadCreateIndex:       nomad.CreateIndex,
			LaunchTime:             instance.LaunchTime,
			ScaleInProtocolSupport: observed && candidate.ScaleInProtocolSupport,
			Ready:                  nomad.Ready && member,
			Healthy:                candidate.ServiceStatus == "ready" && instance.HealthStatus == "Healthy",
			Eligible:               nomad.Eligible,
			Draining:               nomad.Draining,
			Terminating:            instance.LifecycleState != "" && instance.LifecycleState != "InService",
		})
		seen[nomad.NodeID] = struct{}{}
	}
	for instanceID, instance := range cloud.Instances {
		if _, found := seen[instanceID]; found {
			continue
		}
		result = append(result, ScaleInNode{NodeID: instanceID, LaunchTime: instance.LaunchTime, Terminating: instance.LifecycleState != "InService"})
	}

	return result
}

func candidateObservationFresh(now, observedAt time.Time) bool {
	if observedAt.IsZero() || observedAt.After(now) {
		return false
	}

	return now.Sub(observedAt) <= scaleInCandidateMaxAge
}

func findNomadNode(nodes []NomadScaleInNode, nodeID string) (NomadScaleInNode, bool) {
	for _, node := range nodes {
		if node.NodeID == nodeID {
			return node, true
		}
	}

	return NomadScaleInNode{}, false
}

func workerIdentityMatches(state WorkerScaleInState, nodeID, serviceID string) bool {
	return state.NodeID == nodeID && state.ServiceInstanceID == serviceID && state.ScaleInProtocolSupport && state.ServiceStatus == "Draining"
}

func workerShutdownReady(state WorkerScaleInState) bool {
	return state.ShutdownReady && state.SandboxListEmpty && state.RunningSandboxes == 0 && state.StartsInFlight == 0 && state.LifecycleCleanupsInFlight == 0 && state.SnapshotUploadsInFlight == 0
}

func scaleInTerminationBlock(snapshot ScaleInASGSnapshot, instanceID string) string {
	if snapshot.DesiredCapacity <= snapshot.MinSize {
		return "asg_at_minimum"
	}
	if snapshot.TerminateSuspended {
		return "terminate_process_suspended"
	}
	if snapshot.ActiveInstanceRefresh {
		return "instance_refresh_active"
	}
	instance, found := snapshot.Instances[instanceID]
	if !found {
		return "instance_not_in_asg"
	}
	if instance.LifecycleState != "InService" || instance.HealthStatus != "Healthy" || instance.ProtectedFromScaleIn {
		return "instance_not_safe"
	}

	return ""
}
