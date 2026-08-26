package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
)

func TestCapacityServiceIsRegisteredOnlyOnInternalGRPC(t *testing.T) {
	t.Parallel()

	internalServer := grpc.NewServer()
	registerInternalGRPCServices(internalServer, nil, "service-token")
	edgeServer := grpc.NewServer()
	registerEdgeGRPCServices(edgeServer, nil, nil)

	serviceName := proxygrpc.CapacityService_ServiceDesc.ServiceName
	require.Contains(t, internalServer.GetServiceInfo(), serviceName)
	require.NotContains(t, edgeServer.GetServiceInfo(), serviceName)
}
