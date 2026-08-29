package placement

import orchestratorgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"

type createFailureKind uint8

const (
	createFailureUnknown createFailureKind = iota
	createFailureRetrySafeGuestReadiness
	createFailureCleanupFailed
)

// classifyCreateFailure is the sole interpreter of the Orchestrator's
// machine-readable create error contract. Missing or malformed details remain
// unknown and therefore retain the existing generic hard-failure behavior.
func classifyCreateFailure(err error) createFailureKind {
	switch {
	case orchestratorgrpc.IsSandboxCreateCleanupFailed(err):
		return createFailureCleanupFailed
	case orchestratorgrpc.IsRetrySafeGuestReadinessTimeout(err):
		return createFailureRetrySafeGuestReadiness
	default:
		return createFailureUnknown
	}
}
