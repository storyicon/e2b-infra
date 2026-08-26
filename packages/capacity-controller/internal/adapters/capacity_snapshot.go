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
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	md.Set(proxygrpc.MetadataAuthorization, "Bearer "+a.serviceToken)
	ctx = metadata.NewOutgoingContext(ctx, md)

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
