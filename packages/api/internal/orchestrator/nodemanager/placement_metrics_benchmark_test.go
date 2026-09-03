package nodemanager

import (
	"strconv"
	"sync/atomic"
	"testing"
)

func BenchmarkPlacementMetricsTryReserveParallel(b *testing.B) {
	for _, benchmark := range []struct {
		name  string
		limit uint64
	}{
		{name: "legacy-unbounded", limit: 0},
		{name: "bounded-16", limit: 16},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			metrics := newPlacementMetrics()
			metrics.SetCreateConcurrencyLimit(benchmark.limit)
			var sequence atomic.Uint64

			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					sandboxID := strconv.FormatUint(sequence.Add(1), 10)
					if metrics.TryReserve(sandboxID, SandboxResources{CPUs: 1, MiBMemory: 512}) {
						metrics.Release(sandboxID)
					}
				}
			})
		})
	}
}
