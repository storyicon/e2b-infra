package nodemanager

import (
	"sync"
	"sync/atomic"

	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

type SandboxResources struct {
	CPUs      int64
	MiBMemory int64
}

const (
	CreateAdmissionStateBounded         = "bounded"
	CreateAdmissionStateLegacyUnbounded = "legacy_unbounded"
)

type PlacementMetrics struct {
	sandboxesInProgress *smap.Map[SandboxResources]
	reservationMu       sync.Mutex
	createLimit         uint64

	createSuccess atomic.Uint64
	createFails   atomic.Uint64
}

func newPlacementMetrics() PlacementMetrics {
	return PlacementMetrics{sandboxesInProgress: smap.New[SandboxResources]()}
}

func (p *PlacementMetrics) Success(sandboxID string) {
	if p.Release(sandboxID) {
		p.createSuccess.Add(1)
	}
}

func (p *PlacementMetrics) Skip(sandboxID string) {
	p.Release(sandboxID)
}

func (p *PlacementMetrics) Fail(sandboxID string) {
	if p.Release(sandboxID) {
		p.createFails.Add(1)
	}
}

func (p *PlacementMetrics) SuccessCount() uint64 {
	return p.createSuccess.Load()
}

func (p *PlacementMetrics) FailsCount() uint64 {
	return p.createFails.Load()
}

func (p *PlacementMetrics) InProgress() map[string]SandboxResources {
	return p.sandboxesInProgress.Items()
}

func (p *PlacementMetrics) InProgressCount() uint32 {
	return uint32(p.sandboxesInProgress.Count())
}

func (p *PlacementMetrics) TryReserve(sandboxID string, resources SandboxResources) bool {
	p.reservationMu.Lock()
	defer p.reservationMu.Unlock()

	if _, exists := p.sandboxesInProgress.Get(sandboxID); exists {
		return false
	}

	if p.createLimit > 0 && uint64(p.sandboxesInProgress.Count()) >= p.createLimit {
		return false
	}

	p.sandboxesInProgress.Insert(sandboxID, resources)

	return true
}

func (p *PlacementMetrics) Release(sandboxID string) bool {
	p.reservationMu.Lock()
	defer p.reservationMu.Unlock()

	if _, exists := p.sandboxesInProgress.Get(sandboxID); !exists {
		return false
	}

	p.sandboxesInProgress.Remove(sandboxID)

	return true
}

func (p *PlacementMetrics) SetCreateConcurrencyLimit(limit uint64) {
	p.reservationMu.Lock()
	defer p.reservationMu.Unlock()

	p.createLimit = limit
}

func (p *PlacementMetrics) CreateConcurrencyLimit() uint64 {
	p.reservationMu.Lock()
	defer p.reservationMu.Unlock()

	return p.createLimit
}

func (p *PlacementMetrics) CreateAdmissionState() string {
	if p.CreateConcurrencyLimit() == 0 {
		return CreateAdmissionStateLegacyUnbounded
	}

	return CreateAdmissionStateBounded
}
