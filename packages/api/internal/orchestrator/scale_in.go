package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
)

var (
	ErrScaleInNodeNotFound    = errors.New("scale-in worker not found")
	ErrScaleInIdentityChanged = nodemanager.ErrScaleInIdentityChanged
	ErrScaleInProtocolMissing = nodemanager.ErrScaleInProtocolMissing
	ErrScaleInStateInvalid    = nodemanager.ErrScaleInStateInvalid
)

type ScaleInCandidate = nodemanager.ScaleInCandidate
type WorkerScaleInState = nodemanager.WorkerScaleInState

func (o *Orchestrator) ListScaleInCandidates(_ context.Context, clusterIDRaw string) ([]ScaleInCandidate, error) {
	clusterID, err := uuid.Parse(clusterIDRaw)
	if err != nil {
		return nil, fmt.Errorf("invalid cluster ID: %w", err)
	}

	candidates := make([]ScaleInCandidate, 0)
	for _, node := range o.GetClusterNodes(clusterID) {
		// The capacity controller needs a Nomad node identity to establish
		// operation ownership before changing worker admission.
		if !node.IsNomadManaged() {
			continue
		}

		candidates = append(candidates, node.ScaleInCandidate())
	}
	slices.SortFunc(candidates, func(a, b ScaleInCandidate) int {
		return compareStrings(a.NodeID, b.NodeID)
	})

	return candidates, nil
}

func (o *Orchestrator) BeginWorkerScaleIn(ctx context.Context, clusterIDRaw, nodeID, expectedServiceID string) (WorkerScaleInState, error) {
	node, err := o.scaleInNode(clusterIDRaw, nodeID)
	if err != nil {
		return WorkerScaleInState{}, err
	}

	return node.BeginWorkerScaleIn(ctx, expectedServiceID)
}

func (o *Orchestrator) VerifyWorkerScaleIn(ctx context.Context, clusterIDRaw, nodeID, expectedServiceID string) (WorkerScaleInState, error) {
	node, err := o.scaleInNode(clusterIDRaw, nodeID)
	if err != nil {
		return WorkerScaleInState{}, err
	}

	state, err := node.ReadWorkerScaleInState(ctx, expectedServiceID)
	if err != nil {
		return WorkerScaleInState{}, err
	}
	if state.ServiceStatus != orchestratorinfo.ServiceInfoStatus_Draining {
		return WorkerScaleInState{}, fmt.Errorf("%w: worker is not draining", ErrScaleInStateInvalid)
	}

	return state, nil
}

func (o *Orchestrator) CancelWorkerScaleIn(ctx context.Context, clusterIDRaw, nodeID, expectedServiceID string) (WorkerScaleInState, error) {
	node, err := o.scaleInNode(clusterIDRaw, nodeID)
	if err != nil {
		return WorkerScaleInState{}, err
	}

	return node.CancelWorkerScaleIn(ctx, expectedServiceID)
}

func (o *Orchestrator) scaleInNode(clusterIDRaw, nodeID string) (*nodemanager.Node, error) {
	clusterID, err := uuid.Parse(clusterIDRaw)
	if err != nil {
		return nil, fmt.Errorf("invalid cluster ID: %w", err)
	}
	if nodeID == "" {
		return nil, fmt.Errorf("%w: empty node ID", ErrScaleInNodeNotFound)
	}

	node := o.GetNode(clusterID, nodeID)
	if node == nil || !node.IsNomadManaged() {
		return nil, fmt.Errorf("%w: %s", ErrScaleInNodeNotFound, nodeID)
	}

	return node, nil
}

func compareStrings(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
