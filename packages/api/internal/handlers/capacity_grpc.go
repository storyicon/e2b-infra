package handlers

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
)

type capacitySnapshotProvider interface {
	CapacitySnapshot(ctx context.Context, clusterID string) (orchestrator.CapacitySnapshot, error)
}

type workerScaleInProvider interface {
	ListScaleInCandidates(ctx context.Context, clusterID string) ([]orchestrator.ScaleInCandidate, error)
	BeginWorkerScaleIn(ctx context.Context, clusterID, nodeID, expectedServiceID string) (orchestrator.WorkerScaleInState, error)
	VerifyWorkerScaleIn(ctx context.Context, clusterID, nodeID, expectedServiceID string) (orchestrator.WorkerScaleInState, error)
	CancelWorkerScaleIn(ctx context.Context, clusterID, nodeID, expectedServiceID string) (orchestrator.WorkerScaleInState, error)
}

type capacityServiceProvider interface {
	capacitySnapshotProvider
	workerScaleInProvider
}

type CapacityService struct {
	proxygrpc.UnimplementedCapacityServiceServer

	provider capacityServiceProvider
	token    string
}

func NewCapacityService(provider capacityServiceProvider, token string) *CapacityService {
	return &CapacityService{provider: provider, token: token}
}

func (s *CapacityService) ListScaleInCandidates(ctx context.Context, request *proxygrpc.ListScaleInCandidatesRequest) (*proxygrpc.ListScaleInCandidatesResponse, error) {
	if !hasCapacityServiceBearer(ctx, s.token) {
		return nil, status.Error(codes.PermissionDenied, "permission denied")
	}
	clusterID, err := validatedClusterID(request)
	if err != nil {
		return nil, err
	}
	if s.provider == nil {
		return nil, status.Error(codes.Unavailable, "scale-in candidates are unavailable")
	}

	candidates, err := s.provider.ListScaleInCandidates(ctx, clusterID)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "scale-in candidates are unavailable")
	}

	response := &proxygrpc.ListScaleInCandidatesResponse{Candidates: make([]*proxygrpc.WorkerScaleInCandidate, 0, len(candidates))}
	for _, candidate := range candidates {
		response.Candidates = append(response.Candidates, &proxygrpc.WorkerScaleInCandidate{
			NodeId:                 candidate.NodeID,
			NomadNodeId:            candidate.NomadNodeID,
			ServiceInstanceId:      candidate.ServiceInstanceID,
			ServiceStatus:          string(candidate.ServiceStatus),
			RunningSandboxes:       candidate.RunningSandboxes,
			CachedStartsInFlight:   candidate.CachedStartsInFlight,
			ApiPlacementInProgress: candidate.APIPlacementInProgress,
			SafeScaleInSupported:   candidate.ProtocolSupported,
			ObservedAt:             timestampOrNil(candidate.ObservedAt),
		})
	}

	return response, nil
}

func (s *CapacityService) BeginWorkerScaleIn(ctx context.Context, request *proxygrpc.WorkerScaleInRequest) (*proxygrpc.WorkerScaleInState, error) {
	return s.runWorkerScaleIn(ctx, request, "begin", func(ctx context.Context, clusterID, nodeID, expectedServiceID string) (orchestrator.WorkerScaleInState, error) {
		return s.provider.BeginWorkerScaleIn(ctx, clusterID, nodeID, expectedServiceID)
	})
}

func (s *CapacityService) VerifyWorkerScaleIn(ctx context.Context, request *proxygrpc.WorkerScaleInRequest) (*proxygrpc.WorkerScaleInState, error) {
	return s.runWorkerScaleIn(ctx, request, "verify", func(ctx context.Context, clusterID, nodeID, expectedServiceID string) (orchestrator.WorkerScaleInState, error) {
		return s.provider.VerifyWorkerScaleIn(ctx, clusterID, nodeID, expectedServiceID)
	})
}

func (s *CapacityService) CancelWorkerScaleIn(ctx context.Context, request *proxygrpc.WorkerScaleInRequest) (*proxygrpc.WorkerScaleInState, error) {
	return s.runWorkerScaleIn(ctx, request, "cancel", func(ctx context.Context, clusterID, nodeID, expectedServiceID string) (orchestrator.WorkerScaleInState, error) {
		return s.provider.CancelWorkerScaleIn(ctx, clusterID, nodeID, expectedServiceID)
	})
}

type workerScaleInCall func(context.Context, string, string, string) (orchestrator.WorkerScaleInState, error)

func (s *CapacityService) runWorkerScaleIn(ctx context.Context, request *proxygrpc.WorkerScaleInRequest, operation string, call workerScaleInCall) (*proxygrpc.WorkerScaleInState, error) {
	if !hasCapacityServiceBearer(ctx, s.token) {
		return nil, status.Error(codes.PermissionDenied, "permission denied")
	}
	clusterID, nodeID, expectedServiceID, err := validatedWorkerScaleInRequest(request)
	if err != nil {
		return nil, err
	}
	if s.provider == nil || call == nil {
		return nil, status.Error(codes.Unavailable, "worker scale-in is unavailable")
	}

	state, err := call(ctx, clusterID, nodeID, expectedServiceID)
	if err != nil {
		return nil, workerScaleInStatus(operation, err)
	}

	return workerScaleInResponse(state), nil
}

func validatedClusterID(request *proxygrpc.ListScaleInCandidatesRequest) (string, error) {
	if request == nil {
		return "", status.Error(codes.InvalidArgument, "request is required")
	}
	clusterID, err := uuid.Parse(request.GetClusterId())
	if err != nil {
		return "", status.Error(codes.InvalidArgument, "invalid cluster ID")
	}

	return clusterID.String(), nil
}

func validatedWorkerScaleInRequest(request *proxygrpc.WorkerScaleInRequest) (string, string, string, error) {
	if request == nil {
		return "", "", "", status.Error(codes.InvalidArgument, "request is required")
	}
	clusterID, err := uuid.Parse(request.GetClusterId())
	if err != nil {
		return "", "", "", status.Error(codes.InvalidArgument, "invalid cluster ID")
	}
	if strings.TrimSpace(request.GetNodeId()) == "" {
		return "", "", "", status.Error(codes.InvalidArgument, "node ID is required")
	}
	if strings.TrimSpace(request.GetExpectedServiceInstanceId()) == "" {
		return "", "", "", status.Error(codes.InvalidArgument, "expected service instance ID is required")
	}

	return clusterID.String(), request.GetNodeId(), request.GetExpectedServiceInstanceId(), nil
}

func workerScaleInStatus(operation string, err error) error {
	switch {
	case errors.Is(err, orchestrator.ErrScaleInNodeNotFound):
		return status.Error(codes.NotFound, "worker not found")
	case errors.Is(err, orchestrator.ErrScaleInIdentityChanged):
		return status.Error(codes.FailedPrecondition, "worker identity changed")
	case errors.Is(err, orchestrator.ErrScaleInProtocolMissing):
		return status.Error(codes.FailedPrecondition, "worker does not support safe scale-in")
	case errors.Is(err, orchestrator.ErrScaleInStateInvalid):
		return status.Error(codes.FailedPrecondition, "worker scale-in state is inconsistent")
	default:
		return status.Errorf(codes.Unavailable, "%s worker scale-in unavailable", operation)
	}
}

func workerScaleInResponse(state orchestrator.WorkerScaleInState) *proxygrpc.WorkerScaleInState {
	return &proxygrpc.WorkerScaleInState{
		NodeId:                    state.NodeID,
		ServiceInstanceId:         state.ServiceInstanceID,
		ServiceStatus:             state.ServiceStatus.String(),
		SafeScaleInSupported:      state.ProtocolSupported,
		RunningSandboxes:          state.RunningSandboxes,
		StartsInFlight:            state.StartsInFlight,
		LifecycleCleanupsInFlight: state.LifecycleCleanupsInFlight,
		SnapshotUploadsInFlight:   state.SnapshotUploadsInFlight,
		ShutdownReady:             state.ShutdownReady,
		SandboxListEmpty:          state.SandboxListEmpty,
		ObservedAt:                timestampOrNil(state.ObservedAt),
	}
}

func timestampOrNil(value time.Time) *timestamppb.Timestamp {
	if value.IsZero() {
		return nil
	}

	return timestamppb.New(value)
}

func (s *CapacityService) GetCapacitySnapshot(ctx context.Context, request *proxygrpc.CapacitySnapshotRequest) (*proxygrpc.CapacitySnapshotResponse, error) {
	if !hasCapacityServiceBearer(ctx, s.token) {
		return nil, status.Error(codes.PermissionDenied, "permission denied")
	}
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	clusterID, err := uuid.Parse(request.GetClusterId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid cluster ID")
	}
	if s.provider == nil {
		return nil, status.Error(codes.Unavailable, "capacity snapshot is unavailable")
	}

	snapshot, err := s.provider.CapacitySnapshot(ctx, clusterID.String())
	if err != nil {
		return nil, status.Error(codes.Unavailable, "capacity snapshot is unavailable")
	}

	return &proxygrpc.CapacitySnapshotResponse{
		WorkloadCount:     snapshot.WorkloadCount,
		RunningCount:      snapshot.RunningCount,
		ActiveIntentCount: snapshot.ActiveIntentCount,
		OverlapCount:      snapshot.OverlapCount,
	}, nil
}

func hasCapacityServiceBearer(ctx context.Context, expectedToken string) bool {
	if expectedToken == "" {
		return false
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	values := md.Get(proxygrpc.MetadataAuthorization)
	if len(values) != 1 {
		return false
	}
	providedToken, ok := strings.CutPrefix(values[0], "Bearer ")
	if !ok || providedToken == "" {
		return false
	}

	providedDigest := sha256.Sum256([]byte(providedToken))
	expectedDigest := sha256.Sum256([]byte(expectedToken))

	return subtle.ConstantTimeCompare(providedDigest[:], expectedDigest[:]) == 1
}

func (a *APIStore) CapacitySnapshot(ctx context.Context, clusterID string) (orchestrator.CapacitySnapshot, error) {
	return a.orchestrator.CapacitySnapshot(ctx, clusterID)
}

func (a *APIStore) ListScaleInCandidates(ctx context.Context, clusterID string) ([]orchestrator.ScaleInCandidate, error) {
	return a.orchestrator.ListScaleInCandidates(ctx, clusterID)
}

func (a *APIStore) BeginWorkerScaleIn(ctx context.Context, clusterID, nodeID, expectedServiceID string) (orchestrator.WorkerScaleInState, error) {
	return a.orchestrator.BeginWorkerScaleIn(ctx, clusterID, nodeID, expectedServiceID)
}

func (a *APIStore) VerifyWorkerScaleIn(ctx context.Context, clusterID, nodeID, expectedServiceID string) (orchestrator.WorkerScaleInState, error) {
	return a.orchestrator.VerifyWorkerScaleIn(ctx, clusterID, nodeID, expectedServiceID)
}

func (a *APIStore) CancelWorkerScaleIn(ctx context.Context, clusterID, nodeID, expectedServiceID string) (orchestrator.WorkerScaleInState, error) {
	return a.orchestrator.CancelWorkerScaleIn(ctx, clusterID, nodeID, expectedServiceID)
}
