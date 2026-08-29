package nodemanager

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

const createUnavailableThreshold = 3

func (n *Node) SandboxCreate(ctx context.Context, sbxRequest *orchestrator.SandboxCreateRequest) (*orchestrator.SandboxCreateResponse, error) {
	client, ctx := n.GetSandboxCreateCtx(ctx, sbxRequest)

	response, err := client.Sandbox.Create(ctx, sbxRequest)
	if err == nil {
		n.consecutiveCreateUnavailable.Store(0)
	} else if status.Code(err) == codes.Unavailable && n.consecutiveCreateUnavailable.Add(1) >= createUnavailableThreshold {
		// Protect this API replica from repeatedly selecting a node with
		// consecutive transport failures. This does not evict the worker or
		// change ASG state; a complete successful sync restores it.
		n.markUnhealthyLocal(ctx)
	}

	return response, err
}
