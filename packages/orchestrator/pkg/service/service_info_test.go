//go:build linux

package service

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
)

type mutableCreateLimitProvider struct {
	limit uint64
}

func (p *mutableCreateLimitProvider) SandboxCreateConcurrencyLimit() uint64 {
	return p.limit
}

func TestServiceInfoPublishesCurrentSandboxCreateConcurrencyLimit(t *testing.T) {
	t.Parallel()

	provider := &mutableCreateLimitProvider{limit: 16}
	server := NewInfoService(&ServiceInfo{}, sandbox.NewSandboxesMap(), metrics.NewHostMetrics(), provider)

	response, err := server.ServiceInfo(t.Context(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Equal(t, uint64(16), response.GetSandboxCreateConcurrencyLimit())

	provider.limit = 8
	response, err = server.ServiceInfo(t.Context(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Equal(t, uint64(8), response.GetSandboxCreateConcurrencyLimit(), "ServiceInfo must read the effective runtime limit for each response")
}

func TestServiceStatusOverrideRejectsDrainingToStandby(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{}
	info.SetStatus(t.Context(), orchestratorinfo.ServiceInfoStatus_Draining)
	server := &Server{info: info}

	_, err := server.ServiceStatusOverride(t.Context(), &orchestratorinfo.ServiceStatusChangeRequest{
		ServiceStatus: orchestratorinfo.ServiceInfoStatus_Standby,
	})

	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_Draining, info.GetStatus().Status)
}
