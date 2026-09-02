package adapters

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
)

type fakeCapacityServiceClient struct {
	response *proxygrpc.CapacitySnapshotResponse
	err      error
	request  *proxygrpc.CapacitySnapshotRequest
	metadata metadata.MD
}

func (f *fakeCapacityServiceClient) GetCapacitySnapshot(
	ctx context.Context,
	request *proxygrpc.CapacitySnapshotRequest,
	_ ...grpc.CallOption,
) (*proxygrpc.CapacitySnapshotResponse, error) {
	f.request = request
	f.metadata, _ = metadata.FromOutgoingContext(ctx)

	return f.response, f.err
}

func TestCapacitySnapshotReadsAuthenticatedClusterWorkload(t *testing.T) {
	t.Parallel()

	client := &fakeCapacityServiceClient{response: &proxygrpc.CapacitySnapshotResponse{
		WorkloadCount:     500,
		RunningCount:      100,
		ActiveIntentCount: 450,
		OverlapCount:      50,
	}}
	reader := NewCapacitySnapshot(client, "service-token")
	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs(proxygrpc.MetadataAuthorization, "Bearer stale-token"))

	snapshot, err := reader.Snapshot(ctx, "cluster-1")

	require.NoError(t, err)
	require.Equal(t, int64(500), snapshot.WorkloadCount)
	require.Equal(t, "cluster-1", client.request.GetClusterId())
	require.Equal(t, []string{"Bearer service-token"}, client.metadata.Get(proxygrpc.MetadataAuthorization))
}

func TestCapacitySnapshotReturnsRPCErrorWithoutSubstituteData(t *testing.T) {
	t.Parallel()

	client := &fakeCapacityServiceClient{err: errors.New("service unavailable")}
	reader := NewCapacitySnapshot(client, "service-token")

	_, err := reader.Snapshot(t.Context(), "cluster-1")

	require.ErrorContains(t, err, "get capacity snapshot")
}

func TestCapacitySnapshotRejectsInvalidResponses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		response *proxygrpc.CapacitySnapshotResponse
	}{
		{name: "nil response"},
		{name: "workload overflows controller count", response: &proxygrpc.CapacitySnapshotResponse{WorkloadCount: math.MaxUint64}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			reader := NewCapacitySnapshot(&fakeCapacityServiceClient{response: tt.response}, "service-token")

			_, err := reader.Snapshot(t.Context(), "cluster-1")

			require.Error(t, err)
		})
	}
}
