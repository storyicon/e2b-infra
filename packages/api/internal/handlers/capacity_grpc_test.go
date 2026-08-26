package handlers

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	sharedauth "github.com/e2b-dev/infra/packages/auth/pkg/auth"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
)

type fakeCapacitySnapshotProvider struct {
	snapshot orchestrator.CapacitySnapshot
	err      error
	calls    int
	cluster  string
}

func (f *fakeCapacitySnapshotProvider) CapacitySnapshot(_ context.Context, clusterID string) (orchestrator.CapacitySnapshot, error) {
	f.calls++
	f.cluster = clusterID

	return f.snapshot, f.err
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

var _ capacitySnapshotProvider = (*fakeCapacitySnapshotProvider)(nil)
