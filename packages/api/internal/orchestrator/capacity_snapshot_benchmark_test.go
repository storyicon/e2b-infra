package orchestrator

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/capacity-demand/startintent"
)

func BenchmarkLegacyCapacitySnapshot10000(b *testing.B) {
	clusterID := uuid.New()
	now := time.Now().UTC()
	intentStore := newFakeStartIntentStore()
	running := make([]sandbox.Sandbox, 0, 5_000)
	for index := range 5_000 {
		intentID := fmt.Sprintf("intent-%d", index)
		intentStore.records[intentID] = startintent.Record{Intent: startintent.Intent{
			ClusterID: clusterID.String(), SandboxID: intentID, OwnerToken: "execution", VCPU: 1, MemoryMiB: 512, Compatibility: startintent.SinglePoolCompatibility,
		}, State: startintent.StateOutstanding, CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
		running = append(running, sandbox.Sandbox{
			SandboxID: fmt.Sprintf("running-%d", index), ClusterID: clusterID, State: sandbox.StateRunning, VCpu: 1, RamMB: 512,
		})
	}
	o := &Orchestrator{
		startIntentStore:      intentStore,
		runningSandboxReader:  fakeRunningSandboxReader{items: running},
		capacityPoolVCPU:      1,
		capacityPoolMemoryMiB: 512,
	}

	b.ResetTimer()
	for b.Loop() {
		if _, err := o.CapacitySnapshot(b.Context(), clusterID.String()); err != nil {
			b.Fatal(err)
		}
	}
}
