package nodemanager

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
)

var (
	ErrScaleInIdentityChanged = errors.New("worker identity changed")
	ErrScaleInProtocolMissing = errors.New("worker does not support safe scale-in")
	ErrScaleInStateInvalid    = errors.New("worker returned inconsistent scale-in state")
)

type scaleInObservation struct {
	NodeID            string
	ServiceInstanceID string
	RunningSandboxes  uint64
	StartsInFlight    uint64
	ProtocolSupported bool
	ObservedAt        time.Time
}

type ScaleInCandidate struct {
	NodeID                 string
	NomadNodeID            string
	ServiceInstanceID      string
	ServiceStatus          api.NodeStatus
	RunningSandboxes       uint64
	CachedStartsInFlight   uint64
	APIPlacementInProgress uint64
	ProtocolSupported      bool
	ObservedAt             time.Time
}

type WorkerScaleInState struct {
	NodeID                    string
	ServiceInstanceID         string
	ServiceStatus             orchestratorinfo.ServiceInfoStatus
	ProtocolSupported         bool
	RunningSandboxes          uint64
	StartsInFlight            uint64
	LifecycleCleanupsInFlight uint64
	SnapshotUploadsInFlight   uint64
	ShutdownReady             bool
	SandboxListEmpty          bool
	ObservedAt                time.Time
}

// updateScaleInObservation stores one internally consistent ServiceInfo
// observation. The snapshot is only a candidate-selection hint; termination
// eligibility always comes from ReadWorkerScaleInState.
func (n *Node) updateScaleInObservation(info *orchestratorinfo.ServiceInfoResponse) {
	if info == nil {
		return
	}

	n.mutex.Lock()
	n.scaleIn = scaleInObservation{
		NodeID:            info.GetNodeId(),
		ServiceInstanceID: info.GetServiceId(),
		RunningSandboxes:  uint64(info.GetMetricSandboxesRunning()),
		StartsInFlight:    info.GetSandboxStartsInFlight(),
		ProtocolSupported: info.GetSafeScaleInSupported(),
		ObservedAt:        time.Now().UTC(),
	}
	n.mutex.Unlock()
}

func (n *Node) ScaleInCandidate() ScaleInCandidate {
	n.mutex.RLock()
	observation := n.scaleIn
	n.mutex.RUnlock()

	return ScaleInCandidate{
		NodeID:                 observation.NodeID,
		NomadNodeID:            n.NomadNodeShortID,
		ServiceInstanceID:      observation.ServiceInstanceID,
		ServiceStatus:          n.Status(),
		RunningSandboxes:       observation.RunningSandboxes,
		CachedStartsInFlight:   observation.StartsInFlight,
		APIPlacementInProgress: uint64(n.PlacementMetrics.InProgressCount()),
		ProtocolSupported:      observation.ProtocolSupported,
		ObservedAt:             observation.ObservedAt,
	}
}

func (n *Node) BeginWorkerScaleIn(ctx context.Context, expectedServiceID string) (WorkerScaleInState, error) {
	if err := n.validateScaleInWorker(ctx, expectedServiceID); err != nil {
		return WorkerScaleInState{}, err
	}

	client, callCtx := n.GetClient(ctx)
	_, err := client.Info.ServiceStatusOverride(callCtx, &orchestratorinfo.ServiceStatusChangeRequest{
		ServiceStatus:     orchestratorinfo.ServiceInfoStatus_Draining,
		ExpectedServiceId: expectedServiceID,
	})
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			return WorkerScaleInState{}, fmt.Errorf("%w: begin drain", ErrScaleInIdentityChanged)
		}

		return WorkerScaleInState{}, fmt.Errorf("set worker draining: %w", err)
	}

	state, err := n.ReadWorkerScaleInState(ctx, expectedServiceID)
	if err != nil {
		return WorkerScaleInState{}, err
	}
	if state.ServiceStatus != orchestratorinfo.ServiceInfoStatus_Draining {
		return WorkerScaleInState{}, fmt.Errorf("%w: worker did not enter draining", ErrScaleInStateInvalid)
	}

	return state, nil
}

func (n *Node) ReadWorkerScaleInState(ctx context.Context, expectedServiceID string) (WorkerScaleInState, error) {
	_, err := n.readScaleInInfo(ctx, expectedServiceID)
	if err != nil {
		return WorkerScaleInState{}, err
	}

	client, callCtx := n.GetClient(ctx)
	listed, err := client.Sandbox.List(callCtx, &emptypb.Empty{})
	if err != nil {
		return WorkerScaleInState{}, fmt.Errorf("list worker sandboxes: %w", err)
	}
	if listed == nil {
		return WorkerScaleInState{}, fmt.Errorf("%w: empty Sandbox.List response", ErrScaleInStateInvalid)
	}

	// Fence Sandbox.List with a second identity-checked ServiceInfo read. Without
	// this, a worker restart between the first Info and List could combine an old
	// process's shutdown-ready state with the replacement process's empty list.
	verifiedInfo, err := n.readScaleInInfo(ctx, expectedServiceID)
	if err != nil {
		return WorkerScaleInState{}, err
	}

	state := workerScaleInState(verifiedInfo, len(listed.GetSandboxes()) == 0)
	if state.ShutdownReady && (state.ServiceStatus != orchestratorinfo.ServiceInfoStatus_Draining ||
		state.RunningSandboxes != 0 ||
		state.StartsInFlight != 0 ||
		state.LifecycleCleanupsInFlight != 0 ||
		state.SnapshotUploadsInFlight != 0 ||
		!state.SandboxListEmpty) {
		return WorkerScaleInState{}, fmt.Errorf("%w: shutdown_ready contradicts worker activity", ErrScaleInStateInvalid)
	}

	return state, nil
}

func (n *Node) CancelWorkerScaleIn(ctx context.Context, expectedServiceID string) (WorkerScaleInState, error) {
	info, err := n.readWorkerInfo(ctx)
	if err != nil {
		return WorkerScaleInState{}, err
	}
	if info.GetNodeId() != n.ID {
		return WorkerScaleInState{}, fmt.Errorf("%w: expected node %q, got node %q", ErrScaleInIdentityChanged, n.ID, info.GetNodeId())
	}
	if !info.GetSafeScaleInSupported() {
		return WorkerScaleInState{}, ErrScaleInProtocolMissing
	}
	if info.GetServiceId() != expectedServiceID {
		// Nomad has already been restored before cancellation reaches the worker.
		// If the old process disappeared and its replacement is independently
		// Healthy, there is no old Draining state left to clear. Return the live
		// replacement identity without mutating it so the controller can retire
		// the stale operation marker.
		if info.GetServiceStatus() != orchestratorinfo.ServiceInfoStatus_Healthy {
			return WorkerScaleInState{}, fmt.Errorf("%w: replacement worker is not healthy", ErrScaleInIdentityChanged)
		}

		return workerScaleInState(info, false), nil
	}

	client, callCtx := n.GetClient(ctx)
	_, err = client.Info.ServiceStatusOverride(callCtx, &orchestratorinfo.ServiceStatusChangeRequest{
		ServiceStatus:     orchestratorinfo.ServiceInfoStatus_Healthy,
		ExpectedServiceId: expectedServiceID,
	})
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			return WorkerScaleInState{}, fmt.Errorf("%w: cancel drain", ErrScaleInIdentityChanged)
		}

		return WorkerScaleInState{}, fmt.Errorf("restore worker healthy: %w", err)
	}

	state, err := n.ReadWorkerScaleInState(ctx, expectedServiceID)
	if err != nil {
		return WorkerScaleInState{}, err
	}
	if state.ServiceStatus != orchestratorinfo.ServiceInfoStatus_Healthy {
		return WorkerScaleInState{}, fmt.Errorf("%w: worker did not return healthy", ErrScaleInStateInvalid)
	}

	return state, nil
}

func (n *Node) validateScaleInWorker(ctx context.Context, expectedServiceID string) error {
	_, err := n.readScaleInInfo(ctx, expectedServiceID)

	return err
}

func (n *Node) readScaleInInfo(ctx context.Context, expectedServiceID string) (*orchestratorinfo.ServiceInfoResponse, error) {
	if expectedServiceID == "" {
		return nil, fmt.Errorf("%w: expected service instance ID is empty", ErrScaleInIdentityChanged)
	}

	info, err := n.readWorkerInfo(ctx)
	if err != nil {
		return nil, err
	}
	if info.GetNodeId() != n.ID || info.GetServiceId() != expectedServiceID {
		return nil, fmt.Errorf("%w: expected node %q service %q, got node %q service %q",
			ErrScaleInIdentityChanged, n.ID, expectedServiceID, info.GetNodeId(), info.GetServiceId())
	}
	if !info.GetSafeScaleInSupported() {
		return nil, ErrScaleInProtocolMissing
	}

	return info, nil
}

func (n *Node) readWorkerInfo(ctx context.Context) (*orchestratorinfo.ServiceInfoResponse, error) {
	client, callCtx := n.GetClient(ctx)
	info, err := client.Info.ServiceInfo(callCtx, &emptypb.Empty{})
	if err != nil {
		return nil, fmt.Errorf("read worker service info: %w", err)
	}
	if info == nil {
		return nil, fmt.Errorf("%w: empty ServiceInfo response", ErrScaleInStateInvalid)
	}

	return info, nil
}

func workerScaleInState(info *orchestratorinfo.ServiceInfoResponse, sandboxListEmpty bool) WorkerScaleInState {
	return WorkerScaleInState{
		NodeID:                    info.GetNodeId(),
		ServiceInstanceID:         info.GetServiceId(),
		ServiceStatus:             info.GetServiceStatus(),
		ProtocolSupported:         info.GetSafeScaleInSupported(),
		RunningSandboxes:          uint64(info.GetMetricSandboxesRunning()),
		StartsInFlight:            info.GetSandboxStartsInFlight(),
		LifecycleCleanupsInFlight: info.GetLifecycleCleanupsInFlight(),
		SnapshotUploadsInFlight:   info.GetSnapshotUploadsInFlight(),
		ShutdownReady:             info.GetShutdownReady(),
		SandboxListEmpty:          sandboxListEmpty,
		ObservedAt:                time.Now().UTC(),
	}
}
