package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/placement"
	capacitydemand "github.com/e2b-dev/infra/packages/shared/pkg/capacity-demand"
	"github.com/e2b-dev/infra/packages/shared/pkg/capacity-demand/startintent"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/machineinfo"
)

const (
	defaultStartIntentLeaseTTL          = 30 * time.Second
	defaultStartIntentHeartbeatInterval = 10 * time.Second
	defaultStartIntentHandoffTTL        = 30 * time.Second
	startIntentCleanupTimeout           = 2 * time.Second
)

var benchmarkRunIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

type startIntentStore interface {
	Upsert(ctx context.Context, intent startintent.Intent, now, expiresAt time.Time) (bool, error)
	Heartbeat(ctx context.Context, clusterID, sandboxID, ownerToken string, now, expiresAt time.Time) (bool, error)
	Handoff(ctx context.Context, clusterID, sandboxID, ownerToken string, now, expiresAt time.Time) (bool, error)
	Remove(ctx context.Context, clusterID, sandboxID, ownerToken string) (bool, error)
	Active(ctx context.Context, clusterID string, now time.Time) ([]startintent.Record, error)
}

type startIntentLease struct {
	store       startIntentStore
	intent      startintent.Intent
	leaseTTL    time.Duration
	handoffTTL  time.Duration
	ctx         context.Context //nolint:containedctx // the lease owns a request-derived heartbeat context
	cancel      context.CancelFunc
	heartbeatWG sync.WaitGroup

	errMu sync.Mutex
	err   error

	stopOnce   sync.Once
	removeOnce sync.Once
}

func beginStartIntent(
	ctx context.Context,
	store startIntentStore,
	intent startintent.Intent,
	leaseTTL time.Duration,
	heartbeatInterval time.Duration,
	handoffTTL time.Duration,
) (*startIntentLease, error) {
	if store == nil {
		return nil, errors.New("start intent store is required")
	}
	if leaseTTL <= 0 || heartbeatInterval <= 0 || handoffTTL <= 0 {
		return nil, errors.New("start intent lease durations must be positive")
	}

	now := time.Now().UTC()
	created, err := store.Upsert(ctx, intent, now, now.Add(leaseTTL))
	if err != nil {
		return nil, fmt.Errorf("persist start intent: %w", err)
	}
	if !created {
		return nil, errors.New("persist start intent: sandbox already has an active owner")
	}

	leaseCtx, cancel := context.WithCancel(ctx)
	lease := &startIntentLease{
		store:      store,
		intent:     intent,
		leaseTTL:   leaseTTL,
		handoffTTL: handoffTTL,
		ctx:        leaseCtx,
		cancel:     cancel,
	}
	lease.heartbeatWG.Add(1)
	go lease.heartbeat(heartbeatInterval)

	return lease, nil
}

func (l *startIntentLease) heartbeat(interval time.Duration) {
	defer l.heartbeatWG.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-l.ctx.Done():
			return
		case <-ticker.C:
		}

		now := time.Now().UTC()
		updated, err := l.store.Heartbeat(l.ctx, l.intent.ClusterID, l.intent.SandboxID, l.intent.OwnerToken, now, now.Add(l.leaseTTL))
		if l.ctx.Err() != nil {
			return
		}
		if err == nil && updated {
			continue
		}
		if err == nil {
			err = errors.New("start intent lease is no longer owned by this request")
		}
		l.setError(fmt.Errorf("refresh start intent: %w", err))
		l.cancel()

		return
	}
}

func (l *startIntentLease) Context() context.Context {
	return l.ctx
}

func (l *startIntentLease) Err() error {
	l.errMu.Lock()
	defer l.errMu.Unlock()

	return l.err
}

func (l *startIntentLease) setError(err error) {
	l.errMu.Lock()
	defer l.errMu.Unlock()
	if l.err == nil {
		l.err = err
	}
}

func (l *startIntentLease) stopHeartbeat() {
	l.stopOnce.Do(func() {
		l.cancel()
		l.heartbeatWG.Wait()
	})
}

func (l *startIntentLease) Handoff(requestCtx context.Context) error {
	l.stopHeartbeat()
	if err := l.Err(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(requestCtx), startIntentCleanupTimeout)
	defer cancel()
	now := time.Now().UTC()
	updated, err := l.store.Handoff(ctx, l.intent.ClusterID, l.intent.SandboxID, l.intent.OwnerToken, now, now.Add(l.handoffTTL))
	if err != nil {
		return fmt.Errorf("handoff start intent: %w", err)
	}
	if !updated {
		return errors.New("handoff start intent: lease is no longer owned by this request")
	}

	return nil
}

func (l *startIntentLease) Remove(requestCtx context.Context) {
	l.stopHeartbeat()
	l.removeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(requestCtx), startIntentCleanupTimeout)
		defer cancel()
		if _, err := l.store.Remove(ctx, l.intent.ClusterID, l.intent.SandboxID, l.intent.OwnerToken); err != nil {
			logger.L().Error(requestCtx, "failed to remove start intent",
				zap.Error(err),
				zap.String("cluster_id", l.intent.ClusterID),
				logger.WithSandboxID(l.intent.SandboxID),
			)
		}
	})
}

func startIntentCompatibility(cpu placement.CPURequirement, poolCPU machineinfo.MachineInfo, labelFilteringEnabled bool, labels []string) string {
	buildCompatible := cpu.Build.CPUArchitecture == "" || cpu.Build.IsCompatibleWith(poolCPU)
	pinCompatible := cpu.PinnedModel == "" || cpu.PinnedModel == poolCPU.CPUModel
	if buildCompatible && pinCompatible && (!labelFilteringEnabled || len(labels) == 0) {
		return startintent.SinglePoolCompatibility
	}

	canonicalLabels := append([]string(nil), labels...)
	slices.Sort(canonicalLabels)
	canonical := "build_cpu=" + cpu.Build.CPUArchitecture + "/" + cpu.Build.CPUFamily + "/" + cpu.Build.CPUModel +
		";pinned_cpu=" + cpu.PinnedModel + ";labels="
	if labelFilteringEnabled {
		canonical += strings.Join(canonicalLabels, ",")
	}
	digest := sha256.Sum256([]byte(canonical))

	return startintent.SinglePoolCompatibility + ":" + hex.EncodeToString(digest[:])
}

func validateStartIntentPool(intent startintent.Intent, poolVCPU, poolMemoryMiB int64) error {
	if intent.Compatibility == startintent.SinglePoolCompatibility && intent.VCPU == poolVCPU && intent.MemoryMiB == poolMemoryMiB {
		return nil
	}

	return fmt.Errorf(
		"sandbox request vCPU=%d memoryMiB=%d compatibility=%q does not match autoscaled pool vCPU=%d memoryMiB=%d compatibility=%q",
		intent.VCPU,
		intent.MemoryMiB,
		intent.Compatibility,
		poolVCPU,
		poolMemoryMiB,
		startintent.SinglePoolCompatibility,
	)
}

func usesStartIntents(mode cfg.SandboxCapacityDemandMode) bool {
	return mode == cfg.SandboxCapacityDemandModeDualWrite ||
		mode == cfg.SandboxCapacityDemandModeStartIntentV1 ||
		mode == cfg.SandboxCapacityDemandModeWorkloadV2Shadow
}

func usesWorkloadLedger(mode cfg.SandboxCapacityDemandMode) bool {
	return mode == cfg.SandboxCapacityDemandModeWorkloadV2Shadow || mode == cfg.SandboxCapacityDemandModeWorkloadV2
}

func (o *Orchestrator) recordStartIntentLifecycle(ctx context.Context, stage, outcome string) {
	if o.startIntentLifecycleCounter == nil {
		return
	}

	o.startIntentLifecycleCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("stage", stage),
		attribute.String("outcome", outcome),
		attribute.String("mode", string(o.capacityDemandMode)),
	))
}

func capacityAdmissionMessage(mode cfg.SandboxCapacityDemandMode) string {
	if mode == cfg.SandboxCapacityDemandModeWorkloadV2 {
		return "sandbox workload lease admitted"
	}

	return "sandbox start intent admitted"
}

func (o *Orchestrator) recordCapacityAdmission(ctx context.Context, metadata map[string]string) {
	fields := []zap.Field{zap.String("capacity_mode", string(o.capacityDemandMode))}
	if runHash, ok := benchmarkRunHashFromMetadata(metadata); ok {
		fields = append(fields, zap.String("benchmark_run_hash", runHash))
	}
	logger.L().Info(ctx, capacityAdmissionMessage(o.capacityDemandMode), fields...)
}

func (o *Orchestrator) recordStartIntentAdmission(ctx context.Context, metadata map[string]string) {
	o.recordCapacityAdmission(ctx, metadata)
	o.recordStartIntentLifecycle(ctx, "intent_persisted", "success")
}

func benchmarkRunHashFromMetadata(metadata map[string]string) (string, bool) {
	runID := metadata["benchmarkRunId"]
	if !benchmarkRunIDPattern.MatchString(runID) {
		return "", false
	}

	digest := sha256.Sum256([]byte(runID))

	return hex.EncodeToString(digest[:]), true
}

func waitForStartIntentCapacity(ctx context.Context, timeout, retryInterval time.Duration, try func(context.Context) error) error {
	err := try(ctx)
	if err == nil || !isCapacityUnavailable(err) || timeout <= 0 || retryInterval <= 0 {
		return err
	}

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-waitCtx.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}

			return CapacityWaitTimeoutError{Timeout: timeout}
		case <-ticker.C:
		}

		err = try(waitCtx)
		if err == nil || !isCapacityUnavailable(err) {
			return err
		}
	}
}

func (o *Orchestrator) waitForCapacity(ctx context.Context, demandMode cfg.SandboxCapacityDemandMode, demand startintent.Intent, try func(context.Context) error) error {
	switch demandMode {
	case cfg.SandboxCapacityDemandModeLegacy, cfg.SandboxCapacityDemandModeDualWrite:
		return o.capacityWaiter.Wait(ctx, capacityDemandFromStartIntent(demand), try)
	case cfg.SandboxCapacityDemandModeStartIntentV1, cfg.SandboxCapacityDemandModeWorkloadV2Shadow, cfg.SandboxCapacityDemandModeWorkloadV2:
		return waitForStartIntentCapacity(ctx, o.capacityWaitTimeout, o.capacityRetryInterval, try)
	default:
		return fmt.Errorf("unsupported sandbox capacity demand mode %q", demandMode)
	}
}

func capacityDemandFromStartIntent(intent startintent.Intent) capacitydemand.Demand {
	return capacitydemand.Demand{
		ClusterID: intent.ClusterID,
		SandboxID: intent.SandboxID,
		VCPU:      intent.VCPU,
		MemoryMiB: intent.MemoryMiB,
	}
}
