package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/capacity-demand/startintent"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

type runningSandboxReader interface {
	AllRunningItemsStrict(ctx context.Context) ([]sandbox.Sandbox, error)
}

type CapacitySnapshot struct {
	WorkloadCount     uint64
	RunningCount      uint64
	ActiveIntentCount uint64
	OverlapCount      uint64
}

func (o *Orchestrator) CapacitySnapshot(ctx context.Context, clusterIDRaw string) (CapacitySnapshot, error) {
	clusterID, err := uuid.Parse(clusterIDRaw)
	if err != nil {
		return CapacitySnapshot{}, fmt.Errorf("invalid cluster ID: %w", err)
	}
	if o.startIntentStore == nil {
		return CapacitySnapshot{}, errors.New("start intent store is not configured")
	}
	if o.runningSandboxReader == nil {
		return CapacitySnapshot{}, errors.New("running sandbox reader is not configured")
	}
	if o.capacityPoolVCPU <= 0 || o.capacityPoolMemoryMiB <= 0 {
		return CapacitySnapshot{}, errors.New("capacity snapshot pool resources are not configured")
	}

	now := time.Now().UTC()
	intents, err := o.startIntentStore.Active(ctx, clusterID.String(), now)
	if err != nil {
		return CapacitySnapshot{}, fmt.Errorf("read active start intents: %w", err)
	}
	for _, intent := range intents {
		if intent.Compatibility != startintent.SinglePoolCompatibility || intent.VCPU != o.capacityPoolVCPU || intent.MemoryMiB != o.capacityPoolMemoryMiB {
			return CapacitySnapshot{}, fmt.Errorf(
				"incompatible start intent %q: requested vCPU=%d memoryMiB=%d compatibility=%q, supported vCPU=%d memoryMiB=%d compatibility=%q",
				intent.SandboxID,
				intent.VCPU,
				intent.MemoryMiB,
				intent.Compatibility,
				o.capacityPoolVCPU,
				o.capacityPoolMemoryMiB,
				startintent.SinglePoolCompatibility,
			)
		}
	}
	running, err := o.runningSandboxReader.AllRunningItemsStrict(ctx)
	if err != nil {
		return CapacitySnapshot{}, fmt.Errorf("read running sandboxes: %w", err)
	}

	runningIDs := make(map[string]struct{})
	for _, sbx := range running {
		if sbx.ClusterID == clusterID && sbx.State == sandbox.StateRunning {
			if sbx.VCpu != o.capacityPoolVCPU || sbx.RamMB != o.capacityPoolMemoryMiB {
				return CapacitySnapshot{}, fmt.Errorf(
					"incompatible running sandbox %q: vCPU=%d memoryMiB=%d, supported vCPU=%d memoryMiB=%d",
					sbx.SandboxID,
					sbx.VCpu,
					sbx.RamMB,
					o.capacityPoolVCPU,
					o.capacityPoolMemoryMiB,
				)
			}
			runningIDs[sbx.SandboxID] = struct{}{}
		}
	}
	intentIDs := make(map[string]struct{}, len(intents))
	observedHandoffs := make([]startintent.Record, 0)
	overlapCount := uint64(0)
	for _, intent := range intents {
		intentIDs[intent.SandboxID] = struct{}{}
		if _, ok := runningIDs[intent.SandboxID]; ok {
			overlapCount++
			if intent.State == startintent.StateHandoff {
				observedHandoffs = append(observedHandoffs, intent)
			}
		}
	}

	workloadIDs := make(map[string]struct{}, len(runningIDs)+len(intentIDs))
	for sandboxID := range runningIDs {
		workloadIDs[sandboxID] = struct{}{}
	}
	for sandboxID := range intentIDs {
		workloadIDs[sandboxID] = struct{}{}
	}

	snapshot := CapacitySnapshot{
		WorkloadCount:     uint64(len(workloadIDs)),
		RunningCount:      uint64(len(runningIDs)),
		ActiveIntentCount: uint64(len(intentIDs)),
		OverlapCount:      overlapCount,
	}
	if len(observedHandoffs) > 0 {
		go o.cleanupObservedHandoffs(ctx, observedHandoffs)
	}

	return snapshot, nil
}

func (o *Orchestrator) cleanupObservedHandoffs(requestCtx context.Context, intents []startintent.Record) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(requestCtx), startIntentCleanupTimeout)
	defer cancel()

	for _, intent := range intents {
		if err := ctx.Err(); err != nil {
			return
		}
		if _, err := o.startIntentStore.Remove(ctx, intent.ClusterID, intent.SandboxID, intent.OwnerToken); err != nil {
			logger.L().Warn(requestCtx, "failed to remove running-observed start intent handoff",
				zap.Error(err),
				zap.String("cluster_id", intent.ClusterID),
				logger.WithSandboxID(intent.SandboxID),
			)
		}
	}
}
