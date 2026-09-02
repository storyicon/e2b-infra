package handlers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	sharedauth "github.com/e2b-dev/infra/packages/auth/pkg/auth"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
)

type fakeCapacitySnapshotProvider struct {
	snapshot   orchestrator.CapacitySnapshot
	err        error
	calls      int
	cluster    string
	candidates []orchestrator.ScaleInCandidate
	state      orchestrator.WorkerScaleInState
}

func (f *fakeCapacitySnapshotProvider) CapacitySnapshot(_ context.Context, clusterID string) (orchestrator.CapacitySnapshot, error) {
	f.calls++
	f.cluster = clusterID

	return f.snapshot, f.err
}

func (f *fakeCapacitySnapshotProvider) ListScaleInCandidates(_ context.Context, clusterID string) ([]orchestrator.ScaleInCandidate, error) {
	f.calls++
	f.cluster = clusterID

	return f.candidates, f.err
}

func (f *fakeCapacitySnapshotProvider) BeginWorkerScaleIn(_ context.Context, clusterID, _, _ string) (orchestrator.WorkerScaleInState, error) {
	f.calls++
	f.cluster = clusterID

	return f.state, f.err
}

func (f *fakeCapacitySnapshotProvider) VerifyWorkerScaleIn(_ context.Context, clusterID, _, _ string) (orchestrator.WorkerScaleInState, error) {
	f.calls++
	f.cluster = clusterID

	return f.state, f.err
}

func (f *fakeCapacitySnapshotProvider) CancelWorkerScaleIn(_ context.Context, clusterID, _, _ string) (orchestrator.WorkerScaleInState, error) {
	f.calls++
	f.cluster = clusterID

	return f.state, f.err
}

func capacityIncomingContext(ctx context.Context, pairs ...string) context.Context {
	return metadata.NewIncomingContext(ctx, metadata.Pairs(pairs...))
}

func TestCapacityServiceRejectsMissingWrongAndCustomerCredentials(t *testing.T) {
	t.Parallel()

	const token = "capacity-service-token"
	tests := []struct {
		name string
		ctx  func(context.Context) context.Context
	}{
		{name: "missing", ctx: func(ctx context.Context) context.Context { return ctx }},
		{name: "wrong bearer", ctx: func(ctx context.Context) context.Context {
			return capacityIncomingContext(ctx, proxygrpc.MetadataAuthorization, "Bearer wrong-token")
		}},
		{name: "ordinary API key", ctx: func(ctx context.Context) context.Context {
			return capacityIncomingContext(ctx, sharedauth.HeaderAPIKey, token)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			provider := &fakeCapacitySnapshotProvider{}
			service := NewCapacityService(provider, token)

			_, err := service.GetCapacitySnapshot(tt.ctx(t.Context()), &proxygrpc.CapacitySnapshotRequest{ClusterId: "00000000-0000-0000-0000-000000000000"})
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			require.Zero(t, provider.calls)
		})
	}
}

func TestCapacityServiceProtectsEveryScaleInRPCWithBearer(t *testing.T) {
	t.Parallel()

	provider := &fakeCapacitySnapshotProvider{}
	service := NewCapacityService(provider, "capacity-service-token")
	request := &proxygrpc.WorkerScaleInRequest{
		ClusterId:                 "00000000-0000-0000-0000-000000000000",
		NodeId:                    "node-1",
		ExpectedServiceInstanceId: "service-1",
	}
	contexts := []context.Context{
		t.Context(),
		capacityIncomingContext(t.Context(), proxygrpc.MetadataAuthorization, "Bearer wrong-token"),
		capacityIncomingContext(t.Context(),
			proxygrpc.MetadataAuthorization, "Bearer capacity-service-token",
			proxygrpc.MetadataAuthorization, "Bearer capacity-service-token",
		),
	}

	for _, ctx := range contexts {
		_, err := service.ListScaleInCandidates(ctx, &proxygrpc.ListScaleInCandidatesRequest{ClusterId: request.GetClusterId()})
		require.Equal(t, codes.PermissionDenied, status.Code(err))
		_, err = service.BeginWorkerScaleIn(ctx, request)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
		_, err = service.VerifyWorkerScaleIn(ctx, request)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
		_, err = service.CancelWorkerScaleIn(ctx, request)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
	}
	require.Zero(t, provider.calls)
}

func TestCapacityServiceReturnsOnlyAggregateSnapshotForValidServiceBearer(t *testing.T) {
	t.Parallel()

	const token = "capacity-service-token"
	provider := &fakeCapacitySnapshotProvider{snapshot: orchestrator.CapacitySnapshot{
		WorkloadCount: 25, RunningCount: 20, ActiveIntentCount: 10, OverlapCount: 5,
	}}
	service := NewCapacityService(provider, token)
	ctx := capacityIncomingContext(t.Context(), proxygrpc.MetadataAuthorization, "Bearer "+token)

	response, err := service.GetCapacitySnapshot(ctx, &proxygrpc.CapacitySnapshotRequest{ClusterId: "00000000-0000-0000-0000-000000000000"})
	require.NoError(t, err)
	require.Equal(t, uint64(25), response.GetWorkloadCount())
	require.Equal(t, uint64(20), response.GetRunningCount())
	require.Equal(t, uint64(10), response.GetActiveIntentCount())
	require.Equal(t, uint64(5), response.GetOverlapCount())
	require.Equal(t, 1, provider.calls)
}

func TestCapacityServiceMapsSnapshotFailureToUnavailable(t *testing.T) {
	t.Parallel()

	provider := &fakeCapacitySnapshotProvider{err: errors.New("redis unavailable")}
	service := NewCapacityService(provider, "capacity-service-token")
	ctx := capacityIncomingContext(t.Context(), proxygrpc.MetadataAuthorization, "Bearer capacity-service-token")

	_, err := service.GetCapacitySnapshot(ctx, &proxygrpc.CapacitySnapshotRequest{ClusterId: "00000000-0000-0000-0000-000000000000"})
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestCapacityServiceReturnsOnlyScaleInCapacityData(t *testing.T) {
	t.Parallel()

	observedAt := time.Date(2026, time.September, 1, 12, 0, 0, 0, time.UTC)
	provider := &fakeCapacitySnapshotProvider{candidates: []orchestrator.ScaleInCandidate{{
		NodeID: "node-1", NomadNodeID: "nomad-1", ServiceInstanceID: "service-1",
		RunningSandboxes: 2, CachedStartsInFlight: 1, APIPlacementInProgress: 3,
		ProtocolSupported: true, ObservedAt: observedAt,
	}}}
	service := NewCapacityService(provider, "capacity-service-token")
	ctx := capacityIncomingContext(t.Context(), proxygrpc.MetadataAuthorization, "Bearer capacity-service-token")

	response, err := service.ListScaleInCandidates(ctx, &proxygrpc.ListScaleInCandidatesRequest{ClusterId: "00000000-0000-0000-0000-000000000000"})
	require.NoError(t, err)
	require.Len(t, response.GetCandidates(), 1)
	candidate := response.GetCandidates()[0]
	require.Equal(t, "node-1", candidate.GetNodeId())
	require.Equal(t, "nomad-1", candidate.GetNomadNodeId())
	require.Equal(t, "service-1", candidate.GetServiceInstanceId())
	require.Equal(t, uint64(2), candidate.GetRunningSandboxes())
	require.Equal(t, observedAt, candidate.GetObservedAt().AsTime())

	// The protobuf descriptor itself proves this private contract has no fields
	// capable of carrying Sandbox or customer identity.
	fields := candidate.ProtoReflect().Descriptor().Fields()
	require.Nil(t, fields.ByName("sandbox_id"))
	require.Nil(t, fields.ByName("team_id"))
	require.Nil(t, fields.ByName("execution_id"))
}

var _ capacityServiceProvider = (*fakeCapacitySnapshotProvider)(nil)
