//go:build linux

package server

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service"
	orchestratorgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
)

func TestCreateRejectsDrainingBeforeAccessingStartResources(t *testing.T) {
	t.Parallel()

	for _, snapshot := range []bool{false, true} {
		t.Run(map[bool]string{false: "create", true: "resume"}[snapshot], func(t *testing.T) {
			info := &service.ServiceInfo{}
			info.SetStatus(t.Context(), orchestratorinfo.ServiceInfoStatus_Draining)
			server := &Server{info: info}

			_, err := server.Create(t.Context(), &orchestratorgrpc.SandboxCreateRequest{
				Sandbox: &orchestratorgrpc.SandboxConfig{Snapshot: snapshot},
			})
			require.Equal(t, codes.ResourceExhausted, status.Code(err))
			require.True(t, orchestratorgrpc.IsSandboxNodeDraining(err))
			require.Zero(t, info.GetAdmissionState().SandboxStartsInFlight)
		})
	}
}
