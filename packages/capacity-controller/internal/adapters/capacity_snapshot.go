package adapters

import (
	"context"
	"errors"
	"fmt"
	"math"

	"google.golang.org/grpc/metadata"

	"github.com/e2b-dev/infra/packages/capacity-controller/internal/controller"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
)

type CapacitySnapshot struct {
	client       proxygrpc.CapacityServiceClient
	serviceToken string
}

func NewCapacitySnapshot(client proxygrpc.CapacityServiceClient, serviceToken string) *CapacitySnapshot {
	return &CapacitySnapshot{client: client, serviceToken: serviceToken}
}

func (a *CapacitySnapshot) Snapshot(ctx context.Context, clusterID string) (controller.CapacitySnapshot, error) {
	ctx = a.authenticatedContext(ctx)

	response, err := a.client.GetCapacitySnapshot(ctx, &proxygrpc.CapacitySnapshotRequest{ClusterId: clusterID})
	if err != nil {
		return controller.CapacitySnapshot{}, fmt.Errorf("get capacity snapshot: %w", err)
	}
	if response == nil {
		return controller.CapacitySnapshot{}, errors.New("get capacity snapshot: empty response")
	}
	if response.GetWorkloadCount() > math.MaxInt64 {
		return controller.CapacitySnapshot{}, errors.New("get capacity snapshot: workload count exceeds supported range")
	}

	return controller.CapacitySnapshot{WorkloadCount: int64(response.GetWorkloadCount())}, nil
}

func (a *CapacitySnapshot) ListScaleInCandidates(ctx context.Context, clusterID string) ([]controller.ScaleInCandidateObservation, error) {
	response, err := a.client.ListScaleInCandidates(a.authenticatedContext(ctx), &proxygrpc.ListScaleInCandidatesRequest{ClusterId: clusterID})
	if err != nil {
		return nil, fmt.Errorf("list scale-in candidates: %w", err)
	}
	if response == nil {
		return nil, errors.New("list scale-in candidates: empty response")
	}

	result := make([]controller.ScaleInCandidateObservation, 0, len(response.GetCandidates()))
	for _, candidate := range response.GetCandidates() {
		if candidate == nil || candidate.GetObservedAt() == nil || !candidate.GetObservedAt().IsValid() {
			return nil, errors.New("list scale-in candidates: invalid candidate")
		}
		running := candidate.GetRunningSandboxes()
		cachedStarts := candidate.GetCachedStartsInFlight()
		placement := candidate.GetApiPlacementInProgress()
		maxWorkload := uint64(math.MaxInt64)
		if running > maxWorkload || cachedStarts > maxWorkload-running || placement > maxWorkload-running-cachedStarts {
			return nil, errors.New("list scale-in candidates: candidate workload exceeds supported range")
		}
		result = append(result, controller.ScaleInCandidateObservation{
			NodeID:                 candidate.GetNodeId(),
			NomadNodeID:            candidate.GetNomadNodeId(),
			ServiceInstanceID:      candidate.GetServiceInstanceId(),
			ServiceStatus:          candidate.GetServiceStatus(),
			RunningSandboxes:       int64(running),
			KnownWorkload:          int64(running + cachedStarts + placement),
			ScaleInProtocolSupport: candidate.GetSafeScaleInSupported(),
			ObservedAt:             candidate.GetObservedAt().AsTime(),
		})
	}

	return result, nil
}

func (a *CapacitySnapshot) BeginWorkerScaleIn(ctx context.Context, clusterID, nodeID, serviceInstanceID, operationID string) (controller.WorkerScaleInState, error) {
	response, err := a.client.BeginWorkerScaleIn(a.authenticatedContext(ctx), scaleInRequest(clusterID, nodeID, serviceInstanceID, operationID))

	return workerScaleInState(response, err, "begin worker scale-in")
}

func (a *CapacitySnapshot) VerifyWorkerScaleIn(ctx context.Context, clusterID, nodeID, serviceInstanceID, operationID string) (controller.WorkerScaleInState, error) {
	response, err := a.client.VerifyWorkerScaleIn(a.authenticatedContext(ctx), scaleInRequest(clusterID, nodeID, serviceInstanceID, operationID))

	return workerScaleInState(response, err, "verify worker scale-in")
}

func (a *CapacitySnapshot) CancelWorkerScaleIn(ctx context.Context, clusterID, nodeID, serviceInstanceID, operationID string) (controller.WorkerScaleInState, error) {
	response, err := a.client.CancelWorkerScaleIn(a.authenticatedContext(ctx), scaleInRequest(clusterID, nodeID, serviceInstanceID, operationID))

	return workerScaleInState(response, err, "cancel worker scale-in")
}

func (a *CapacitySnapshot) authenticatedContext(ctx context.Context) context.Context {
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	md.Set(proxygrpc.MetadataAuthorization, "Bearer "+a.serviceToken)

	return metadata.NewOutgoingContext(ctx, md)
}

func scaleInRequest(clusterID, nodeID, serviceInstanceID, operationID string) *proxygrpc.WorkerScaleInRequest {
	return &proxygrpc.WorkerScaleInRequest{ClusterId: clusterID, NodeId: nodeID, ExpectedServiceInstanceId: serviceInstanceID, OperationId: operationID}
}

func workerScaleInState(response *proxygrpc.WorkerScaleInState, err error, operation string) (controller.WorkerScaleInState, error) {
	if err != nil {
		return controller.WorkerScaleInState{}, fmt.Errorf("%s: %w", operation, err)
	}
	if response == nil || response.GetRunningSandboxes() > math.MaxInt64 || response.GetStartsInFlight() > math.MaxInt64 || response.GetLifecycleCleanupsInFlight() > math.MaxInt64 || response.GetSnapshotUploadsInFlight() > math.MaxInt64 {
		return controller.WorkerScaleInState{}, fmt.Errorf("%s: invalid response", operation)
	}

	return controller.WorkerScaleInState{
		NodeID:                    response.GetNodeId(),
		ServiceInstanceID:         response.GetServiceInstanceId(),
		ServiceStatus:             response.GetServiceStatus(),
		ScaleInProtocolSupport:    response.GetSafeScaleInSupported(),
		RunningSandboxes:          int64(response.GetRunningSandboxes()),
		StartsInFlight:            int64(response.GetStartsInFlight()),
		LifecycleCleanupsInFlight: int64(response.GetLifecycleCleanupsInFlight()),
		SnapshotUploadsInFlight:   int64(response.GetSnapshotUploadsInFlight()),
		ShutdownReady:             response.GetShutdownReady(),
		SandboxListEmpty:          response.GetSandboxListEmpty(),
		ScaleInOperationID:        response.GetOperationId(),
	}, nil
}
