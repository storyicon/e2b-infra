package handlers

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"strings"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
)

type capacitySnapshotProvider interface {
	CapacitySnapshot(ctx context.Context, clusterID string) (orchestrator.CapacitySnapshot, error)
}

type CapacityService struct {
	proxygrpc.UnimplementedCapacityServiceServer

	provider capacitySnapshotProvider
	token    string
}

func NewCapacityService(provider capacitySnapshotProvider, token string) *CapacityService {
	return &CapacityService{provider: provider, token: token}
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
