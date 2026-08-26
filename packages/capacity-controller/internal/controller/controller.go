package controller

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	capacitydemand "github.com/e2b-dev/infra/packages/shared/pkg/capacity-demand"
)

type DemandReader interface {
	Summary(ctx context.Context, clusterID string, now time.Time) (capacitydemand.Summary, error)
}

type Mode string

const (
	ModeLegacyFailureLedger Mode = "legacy-failure-ledger"
	ModeStartIntentV1       Mode = "start-intent-v1"
)

func ParseMode(value string) (Mode, error) {
	mode := Mode(value)
	switch mode {
	case ModeLegacyFailureLedger, ModeStartIntentV1:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid capacity demand mode %q", value)
	}
}

type CapacitySnapshot struct {
	WorkloadCount int64
}

type CapacitySnapshotReader interface {
	Snapshot(ctx context.Context, clusterID string) (CapacitySnapshot, error)
}

type NodeCounter interface {
	ReadyCount(ctx context.Context, nodePool string) (int32, error)
}

type ScaleTarget interface {
	DesiredCapacity(ctx context.Context, asgName string) (int32, error)
	SetDesiredCapacity(ctx context.Context, asgName string, desired int32) error
}

type Config struct {
	Mode             Mode
	ClusterID        string
	NodePool         string
	ASGName          string
	SlotsPerNode     int
	MinNodes         int32
	MaxNodes         int32
	ReconcileTimeout time.Duration
}

type Result struct {
	Mode               Mode
	WorkloadCount      int64
	PendingSandboxes   int64
	TotalFulfilled     int64
	TotalDirectSuccess int64
	BurstDemand        int64
	ReadyNodes         int32
	DesiredNodes       int32
	TargetNodes        int32
	Capped             bool
	Scaled             bool
}

// Reconciler deliberately supports scale-out only. The legacy mode keeps its
// process-local burst accounting for explicit rollback. Start-intent mode is
// level-triggered from an external workload snapshot and ASG desired capacity,
// so it does not carry burst state across reconciliations.
type Reconciler struct {
	config   *Config
	demand   DemandReader
	snapshot CapacitySnapshotReader
	nodes    NodeCounter
	target   ScaleTarget

	mu    sync.Mutex
	burst burst
}

type burst struct {
	active                bool
	baseDesired           int32
	baselineFulfilled     int64
	baselineDirectSuccess int64
}

func New(config *Config, demand DemandReader, snapshot CapacitySnapshotReader, nodes NodeCounter, target ScaleTarget) *Reconciler {
	return &Reconciler{config: config, demand: demand, snapshot: snapshot, nodes: nodes, target: target}
}

func (r *Reconciler) Reconcile(ctx context.Context, now time.Time) (Result, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.config.ReconcileTimeout <= 0 {
		return Result{Mode: r.config.Mode}, errors.New("reconcile timeout must be positive")
	}
	reconcileCtx, cancel := context.WithTimeout(ctx, r.config.ReconcileTimeout)
	defer cancel()

	switch r.config.Mode {
	case ModeLegacyFailureLedger:
		return r.reconcileLegacy(reconcileCtx, now)
	case ModeStartIntentV1:
		return r.reconcileStartIntent(reconcileCtx)
	default:
		return Result{Mode: r.config.Mode}, fmt.Errorf("unsupported capacity demand mode %q", r.config.Mode)
	}
}

func (r *Reconciler) reconcileStartIntent(ctx context.Context) (Result, error) {
	result := Result{Mode: ModeStartIntentV1}
	if r.snapshot == nil {
		return result, errors.New("start-intent capacity snapshot reader is required")
	}

	snapshot, err := r.snapshot.Snapshot(ctx, r.config.ClusterID)
	if err != nil {
		return result, fmt.Errorf("read start-intent capacity snapshot: %w", err)
	}
	if snapshot.WorkloadCount < 0 {
		return result, errors.New("read start-intent capacity snapshot: workload count must be non-negative")
	}
	result.WorkloadCount = snapshot.WorkloadCount

	ready, err := r.nodes.ReadyCount(ctx, r.config.NodePool)
	if err != nil {
		return result, fmt.Errorf("count ready Nomad nodes: %w", err)
	}
	result.ReadyNodes = ready

	desired, err := r.target.DesiredCapacity(ctx, r.config.ASGName)
	if err != nil {
		return result, fmt.Errorf("read ASG desired capacity: %w", err)
	}
	result.DesiredNodes = desired

	required := ceilDiv(snapshot.WorkloadCount, int64(r.config.SlotsPerNode))
	uncapped := max(int64(desired), required, int64(r.config.MinNodes))
	result.Capped = uncapped > int64(r.config.MaxNodes)
	target := clampInt32(uncapped, r.config.MinNodes, r.config.MaxNodes)
	result.TargetNodes = target
	if target <= desired {
		return result, nil
	}

	if err := r.target.SetDesiredCapacity(ctx, r.config.ASGName, target); err != nil {
		return result, fmt.Errorf("set ASG desired capacity to %d: %w", target, err)
	}
	result.Scaled = true

	return result, nil
}

func (r *Reconciler) reconcileLegacy(ctx context.Context, now time.Time) (Result, error) {
	result := Result{Mode: ModeLegacyFailureLedger}
	if r.demand == nil {
		return result, errors.New("legacy capacity demand reader is required")
	}

	summary, err := r.demand.Summary(ctx, r.config.ClusterID, now)
	if err != nil {
		return result, fmt.Errorf("read pending capacity demand: %w", err)
	}
	result.PendingSandboxes = summary.Count
	result.TotalFulfilled = summary.TotalFulfilled
	result.TotalDirectSuccess = summary.TotalDirectSuccess

	ready, err := r.nodes.ReadyCount(ctx, r.config.NodePool)
	if err != nil {
		return result, fmt.Errorf("count ready Nomad nodes: %w", err)
	}
	result.ReadyNodes = ready

	desired, err := r.target.DesiredCapacity(ctx, r.config.ASGName)
	if err != nil {
		return result, fmt.Errorf("read ASG desired capacity: %w", err)
	}
	result.DesiredNodes = desired
	result.TargetNodes = desired
	if summary.Count <= 0 {
		r.burst = burst{}

		return result, nil
	}

	switch {
	case !r.burst.active:
		r.burst = burst{
			active:                true,
			baseDesired:           max(desired, r.config.MinNodes),
			baselineFulfilled:     summary.TotalFulfilled,
			baselineDirectSuccess: summary.TotalDirectSuccess,
		}
	case summary.TotalFulfilled < r.burst.baselineFulfilled:
		return result, fmt.Errorf("capacity demand total fulfilled regressed from %d to %d", r.burst.baselineFulfilled, summary.TotalFulfilled)
	case summary.TotalDirectSuccess < r.burst.baselineDirectSuccess:
		return result, fmt.Errorf("capacity demand total direct success regressed from %d to %d", r.burst.baselineDirectSuccess, summary.TotalDirectSuccess)
	}

	// Direct successes observed while only the burst's baseline nodes are ready
	// consumed capacity the baseline already represents. Once an added node is
	// ready, direct successes consume scale-out capacity and must remain part of
	// the burst even though they never entered the pending ledger.
	if ready <= r.burst.baseDesired {
		r.burst.baselineDirectSuccess = summary.TotalDirectSuccess
	}
	burstDemand := summary.TotalFulfilled - r.burst.baselineFulfilled +
		summary.TotalDirectSuccess - r.burst.baselineDirectSuccess +
		summary.Count
	result.BurstDemand = burstDemand
	additional := ceilDiv(burstDemand, int64(r.config.SlotsPerNode))
	uncapped := int64(r.burst.baseDesired) + additional
	result.Capped = uncapped > int64(r.config.MaxNodes)
	target := clampInt32(uncapped, r.config.MinNodes, r.config.MaxNodes)
	target = max(target, desired)
	result.TargetNodes = target
	if target == desired {
		return result, nil
	}

	if err := r.target.SetDesiredCapacity(ctx, r.config.ASGName, target); err != nil {
		return result, fmt.Errorf("set ASG desired capacity to %d: %w", target, err)
	}
	result.Scaled = true

	return result, nil
}

func ceilDiv(numerator, denominator int64) int64 {
	quotient := numerator / denominator
	if numerator%denominator != 0 {
		quotient++
	}

	return quotient
}

func clampInt32(value int64, minimum, maximum int32) int32 {
	if value < int64(minimum) {
		return minimum
	}
	if value > int64(maximum) {
		return maximum
	}

	return int32(value)
}
