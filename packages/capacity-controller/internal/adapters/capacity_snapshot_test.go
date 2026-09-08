package adapters

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
)

type fakeCapacityServiceClient struct {
	response          *proxygrpc.CapacitySnapshotResponse
	candidateResponse *proxygrpc.ListScaleInCandidatesResponse
	err               error
	request           *proxygrpc.CapacitySnapshotRequest
	workerRequest     *proxygrpc.WorkerScaleInRequest
	workerResponse    *proxygrpc.WorkerScaleInState
	metadata          metadata.MD
}

func (f *fakeCapacityServiceClient) ListScaleInCandidates(context.Context, *proxygrpc.ListScaleInCandidatesRequest, ...grpc.CallOption) (*proxygrpc.ListScaleInCandidatesResponse, error) {
	return f.candidateResponse, f.err
}

func (f *fakeCapacityServiceClient) BeginWorkerScaleIn(_ context.Context, request *proxygrpc.WorkerScaleInRequest, _ ...grpc.CallOption) (*proxygrpc.WorkerScaleInState, error) {
	f.workerRequest = request

	return f.workerResponse, f.err
}

func (f *fakeCapacityServiceClient) VerifyWorkerScaleIn(context.Context, *proxygrpc.WorkerScaleInRequest, ...grpc.CallOption) (*proxygrpc.WorkerScaleInState, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeCapacityServiceClient) CancelWorkerScaleIn(context.Context, *proxygrpc.WorkerScaleInRequest, ...grpc.CallOption) (*proxygrpc.WorkerScaleInState, error) {
	return nil, errors.New("not implemented")
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

func TestListScaleInCandidatesIncludesKnownInFlightWork(t *testing.T) {
	t.Parallel()

	now := timestamppb.Now()
	client := &fakeCapacityServiceClient{candidateResponse: &proxygrpc.ListScaleInCandidatesResponse{
		Candidates: []*proxygrpc.WorkerScaleInCandidate{{
			NodeId: "i-1", RunningSandboxes: 2, CachedStartsInFlight: 3,
			ApiPlacementInProgress: 4, ObservedAt: now,
		}},
	}}

	candidates, err := NewCapacitySnapshot(client, "service-token").ListScaleInCandidates(t.Context(), "cluster-1")

	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, int64(2), candidates[0].RunningSandboxes)
	require.Equal(t, int64(9), candidates[0].KnownWorkload)
}

func TestListScaleInCandidatesRejectsKnownWorkloadOverflow(t *testing.T) {
	t.Parallel()

	client := &fakeCapacityServiceClient{candidateResponse: &proxygrpc.ListScaleInCandidatesResponse{
		Candidates: []*proxygrpc.WorkerScaleInCandidate{{
			RunningSandboxes: math.MaxInt64, CachedStartsInFlight: 1, ObservedAt: timestamppb.Now(),
		}},
	}}

	_, err := NewCapacitySnapshot(client, "service-token").ListScaleInCandidates(t.Context(), "cluster-1")

	require.ErrorContains(t, err, "workload exceeds")
}

func TestBeginWorkerScaleInForwardsStableOperationID(t *testing.T) {
	t.Parallel()
	client := &fakeCapacityServiceClient{workerResponse: &proxygrpc.WorkerScaleInState{
		NodeId: "i-1", ServiceInstanceId: "service-1", ServiceStatus: "Draining",
		SafeScaleInSupported: true, OperationId: "op-1",
	}}

	state, err := NewCapacitySnapshot(client, "service-token").BeginWorkerScaleIn(t.Context(), "cluster-1", "i-1", "service-1", "op-1")

	require.NoError(t, err)
	require.Equal(t, "op-1", client.workerRequest.GetOperationId())
	require.Equal(t, "op-1", state.ScaleInOperationID)
}
