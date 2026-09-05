package adapters

import (
	"context"
	"errors"
	"fmt"
	"time"

	nomadapi "github.com/hashicorp/nomad/api"

	"github.com/e2b-dev/infra/packages/capacity-controller/internal/controller"
)

const (
	nomadMetaOwner       = "e2b_scale_in_owner"
	nomadMetaOperation   = "e2b_scale_in_operation_id"
	nomadMetaServiceID   = "e2b_scale_in_service_instance_id"
	nomadMetaStartedAt   = "e2b_scale_in_started_at"
	nomadMetaStage       = "e2b_scale_in_stage"
	nomadMetaVersion     = "e2b_scale_in_version"
	legacyScaleInVersion = "safe-empty-worker-v1"
)

type Nomad struct {
	client *nomadapi.Client
}

func NewNomad(client *nomadapi.Client) *Nomad {
	return &Nomad{client: client}
}

func (n *Nomad) ReadyCount(ctx context.Context, nodePool string) (int32, error) {
	query := (&nomadapi.QueryOptions{Namespace: "*", AllowStale: false}).WithContext(ctx)
	nodes, _, err := n.client.Nodes().List(query)
	if err != nil {
		return 0, fmt.Errorf("list Nomad nodes: %w", err)
	}

	var ready int32
	for _, node := range nodes {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if node.NodePool == nodePool && node.Status == nomadapi.NodeStatusReady && node.SchedulingEligibility == nomadapi.NodeSchedulingEligible {
			ready++
		}
	}

	return ready, nil
}

func (n *Nomad) Inventory(ctx context.Context, nodePool string) ([]controller.NomadScaleInNode, error) {
	query := (&nomadapi.QueryOptions{Namespace: "*", AllowStale: false}).WithContext(ctx)
	nodes, _, err := n.client.Nodes().List(query)
	if err != nil {
		return nil, fmt.Errorf("list Nomad nodes: %w", err)
	}

	result := make([]controller.NomadScaleInNode, 0, len(nodes))
	for _, node := range nodes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if node.NodePool != nodePool {
			continue
		}
		item := controller.NomadScaleInNode{
			NomadNodeID: node.ID,
			NodeID:      node.Name,
			NodePool:    node.NodePool,
			Ready:       node.Status == nomadapi.NodeStatusReady,
			Eligible:    node.SchedulingEligibility == nomadapi.NodeSchedulingEligible,
			Draining:    node.Drain,
			CreateIndex: node.CreateIndex,
		}
		operation, err := ownedNomadOperation(node.LastDrain)
		if err != nil {
			return nil, fmt.Errorf("read scale-in metadata for Nomad node %q: %w", node.ID, err)
		}
		isolated := operation != nil && hasOwnedNomadIsolation(node.Drain, node.SchedulingEligibility, node.LastDrain, operation, operation.OperationID)
		if isolated {
			item.Draining = true
		} else if operation != nil && !node.Drain && node.SchedulingEligibility == nomadapi.NodeSchedulingEligible && nomadRestoreSourceStage(operation.Stage) {
			if item.Ready {
				// Nomad treats disabling an already-completed drain as a metadata
				// no-op. Eligibility still changes, so derive the durable restoring
				// phase from the live node state and finish the worker rollback.
				operation.Stage = "restoring"
			} else {
				// A down, eligible node has neither owned isolation nor a worker to
				// restore. Keep its historical LastDrain for audit, but do not let it
				// consume active-operation budget or block healthy nodes forever. If
				// the node returns Ready, the same marker resumes as restoring.
				operation = nil
			}
		}
		item.Operation = operation
		result = append(result, item)
	}

	return result, nil
}

func (n *Nomad) MarkDrain(ctx context.Context, node controller.NomadScaleInNode, operation controller.NomadScaleInOperation) error {
	current, err := n.readNode(ctx, node.NomadNodeID)
	if err != nil {
		return err
	}
	owned, err := ownedNomadOperation(current.LastDrain)
	if err != nil {
		return err
	}
	if current.Drain {
		if owned != nil && owned.OperationID == operation.OperationID {
			return nil
		}

		return errors.New("Nomad node already has a non-owned drain")
	}
	if current.Name != node.NodeID || current.NodePool != node.NodePool {
		return errors.New("Nomad node identity changed before drain")
	}

	operation.Stage = "nomad_marked"
	_, err = n.client.Nodes().UpdateDrainOpts(node.NomadNodeID, &nomadapi.DrainOptions{
		DrainSpec: &nomadapi.DrainSpec{
			Deadline:         0,
			IgnoreSystemJobs: true,
		},
		MarkEligible: false,
		Meta:         nomadOperationMeta(operation),
	}, (&nomadapi.WriteOptions{}).WithContext(ctx))
	if err != nil {
		return fmt.Errorf("mark Nomad node draining: %w", err)
	}

	return nil
}

func (n *Nomad) MarkOperationStage(ctx context.Context, node controller.NomadScaleInNode, operation controller.NomadScaleInOperation) error {
	current, err := n.requireOwnedDrain(ctx, node, operation.OperationID)
	if err != nil {
		return err
	}
	_, err = n.client.Nodes().UpdateDrainOpts(current.ID, &nomadapi.DrainOptions{
		DrainSpec:    &nomadapi.DrainSpec{Deadline: 0, IgnoreSystemJobs: true},
		MarkEligible: false,
		Meta:         nomadOperationMeta(operation),
	}, (&nomadapi.WriteOptions{}).WithContext(ctx))
	if err != nil {
		return fmt.Errorf("update owned Nomad drain metadata: %w", err)
	}

	return nil
}

func (n *Nomad) RestoreDrain(ctx context.Context, node controller.NomadScaleInNode, operation controller.NomadScaleInOperation) error {
	current, err := n.requireOwnedDrain(ctx, node, operation.OperationID)
	if err != nil {
		return err
	}
	owned, err := ownedNomadOperation(current.LastDrain)
	if err != nil {
		return err
	}
	if owned == nil || !nomadRestoreSourceStage(owned.Stage) {
		return errors.New("Nomad scale-in operation is no longer reversible")
	}
	operation.Stage = "restoring"
	_, err = n.client.Nodes().UpdateDrainOpts(current.ID, &nomadapi.DrainOptions{
		DrainSpec:    nil,
		MarkEligible: true,
		Meta:         nomadOperationMeta(operation),
	}, (&nomadapi.WriteOptions{}).WithContext(ctx))
	if err != nil {
		return fmt.Errorf("restore owned Nomad drain: %w", err)
	}

	return nil
}

func (n *Nomad) CompleteRestore(ctx context.Context, node controller.NomadScaleInNode, operation controller.NomadScaleInOperation) error {
	current, err := n.readNode(ctx, node.NomadNodeID)
	if err != nil {
		return err
	}
	owned, err := ownedNomadOperation(current.LastDrain)
	if err != nil {
		return err
	}
	if owned == nil || owned.OperationID != operation.OperationID || current.Name != node.NodeID || current.NodePool != node.NodePool {
		return errors.New("Nomad restore ownership changed before completion")
	}

	if owned.Stage != "restored" {
		if current.Drain || current.SchedulingEligibility != nomadapi.NodeSchedulingEligible || !nomadRestoreSourceStage(owned.Stage) {
			return errors.New("Nomad restore ownership changed before completion")
		}

		// A nil-to-nil drain update changes eligibility but Nomad deliberately
		// leaves LastDrain metadata untouched. Start a controller-owned,
		// ignore-system drain so the terminal restored marker is durable. The
		// following disable makes the node eligible again; either call can be
		// retried after a crash by inspecting this marker.
		operation.Stage = "restored"
		_, err = n.client.Nodes().UpdateDrainOpts(current.ID, &nomadapi.DrainOptions{
			DrainSpec:    &nomadapi.DrainSpec{Deadline: 0, IgnoreSystemJobs: true},
			MarkEligible: false,
			Meta:         nomadOperationMeta(operation),
		}, (&nomadapi.WriteOptions{}).WithContext(ctx))
		if err != nil {
			return fmt.Errorf("mark completed Nomad restore: %w", err)
		}
	} else {
		operation.Stage = "restored"
	}

	_, err = n.client.Nodes().UpdateDrainOpts(current.ID, &nomadapi.DrainOptions{
		DrainSpec:    nil,
		MarkEligible: true,
		Meta:         nomadOperationMeta(operation),
	}, (&nomadapi.WriteOptions{}).WithContext(ctx))
	if err != nil {
		return fmt.Errorf("make restored Nomad node eligible: %w", err)
	}

	current, err = n.readNode(ctx, node.NomadNodeID)
	if err != nil {
		return err
	}
	owned, err = ownedNomadOperation(current.LastDrain)
	if err != nil {
		return err
	}
	if current.Drain || current.SchedulingEligibility != nomadapi.NodeSchedulingEligible || owned == nil || owned.OperationID != operation.OperationID || owned.Stage != "restored" {
		return errors.New("Nomad restore did not reach the expected terminal state")
	}

	return nil
}

func (n *Nomad) CompleteTermination(ctx context.Context, node controller.NomadScaleInNode, operation controller.NomadScaleInOperation) error {
	current, err := n.requireOwnedDrain(ctx, node, operation.OperationID)
	if err != nil {
		return err
	}
	operation.Stage = "complete"
	_, err = n.client.Nodes().UpdateDrainOpts(current.ID, &nomadapi.DrainOptions{
		DrainSpec:    &nomadapi.DrainSpec{Deadline: 0, IgnoreSystemJobs: true},
		MarkEligible: false,
		Meta:         nomadOperationMeta(operation),
	}, (&nomadapi.WriteOptions{}).WithContext(ctx))
	if err != nil {
		return fmt.Errorf("complete owned Nomad termination: %w", err)
	}

	return nil
}

func (n *Nomad) requireOwnedDrain(ctx context.Context, node controller.NomadScaleInNode, operationID string) (*nomadapi.Node, error) {
	current, err := n.readNode(ctx, node.NomadNodeID)
	if err != nil {
		return nil, err
	}
	owned, err := ownedNomadOperation(current.LastDrain)
	if err != nil {
		return nil, err
	}
	if !hasOwnedNomadIsolation(current.Drain, current.SchedulingEligibility, current.LastDrain, owned, operationID) || current.Name != node.NodeID || current.NodePool != node.NodePool {
		return nil, errors.New("Nomad drain is not owned by the expected scale-in operation")
	}

	return current, nil
}

func hasOwnedNomadIsolation(drain bool, eligibility string, lastDrain *nomadapi.DrainMetadata, owned *controller.NomadScaleInOperation, operationID string) bool {
	if owned == nil || owned.OperationID != operationID {
		return false
	}
	if drain {
		return true
	}

	// With IgnoreSystemJobs enabled, a worker that only runs the orchestrator
	// system job completes its Nomad drain immediately. Nomad then reports
	// Drain=false while preserving LastDrain metadata and leaving the node
	// ineligible. That completed, owned isolation is equivalent to an active
	// drain for the scale-in transaction; an eligible or canceled node is not.
	return eligibility == nomadapi.NodeSchedulingIneligible &&
		lastDrain != nil && lastDrain.Status == nomadapi.DrainStatusComplete
}

func nomadRestoreSourceStage(stage string) bool {
	switch stage {
	case "nomad_marked", "worker_draining", "restoring":
		return true
	default:
		return false
	}
}

func (n *Nomad) readNode(ctx context.Context, nodeID string) (*nomadapi.Node, error) {
	current, _, err := n.client.Nodes().Info(nodeID, (&nomadapi.QueryOptions{AllowStale: false}).WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("read Nomad node: %w", err)
	}

	return current, nil
}

func ownedNomadOperation(metadata *nomadapi.DrainMetadata) (*controller.NomadScaleInOperation, error) {
	if metadata == nil || metadata.Meta[nomadMetaOwner] != controller.ScaleInNomadOwner {
		return nil, nil
	}
	operationID := metadata.Meta[nomadMetaOperation]
	startedAtValue := metadata.Meta[nomadMetaStartedAt]
	stage := metadata.Meta[nomadMetaStage]
	serviceID := metadata.Meta[nomadMetaServiceID]
	if operationID == "" || serviceID == "" || startedAtValue == "" || stage == "" {
		return nil, errors.New("owned drain has incomplete or unsupported metadata")
	}
	version := metadata.Meta[nomadMetaVersion]
	legacyComplete := version == legacyScaleInVersion && stage == "complete" && metadata.Status == nomadapi.DrainStatusComplete
	if version != controller.ScaleInVersion && !legacyComplete {
		return nil, errors.New("owned drain has incomplete or unsupported metadata")
	}
	startedAt, err := time.Parse(time.RFC3339Nano, startedAtValue)
	if err != nil {
		return nil, fmt.Errorf("parse owned drain start time: %w", err)
	}

	return &controller.NomadScaleInOperation{OperationID: operationID, ServiceInstanceID: serviceID, StartedAt: startedAt, Stage: stage}, nil
}

func nomadOperationMeta(operation controller.NomadScaleInOperation) map[string]string {
	return map[string]string{
		nomadMetaOwner:     controller.ScaleInNomadOwner,
		nomadMetaOperation: operation.OperationID,
		nomadMetaServiceID: operation.ServiceInstanceID,
		nomadMetaStartedAt: operation.StartedAt.UTC().Format(time.RFC3339Nano),
		nomadMetaStage:     operation.Stage,
		nomadMetaVersion:   controller.ScaleInVersion,
	}
}
