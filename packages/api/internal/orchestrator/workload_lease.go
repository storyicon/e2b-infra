package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const workloadMutationTimeout = 2 * time.Second

type workloadLeaseStore interface {
	Acquire(ctx context.Context, clusterID, sandboxID, executionID string, now, expiresAt time.Time) (bool, error)
	Renew(ctx context.Context, clusterID, sandboxID, executionID string, now, expiresAt time.Time) (bool, error)
	Promote(ctx context.Context, clusterID, sandboxID, executionID string, now, expiresAt time.Time) (bool, error)
	UpdateDeadline(ctx context.Context, clusterID, sandboxID, executionID string, now, expiresAt time.Time) (bool, error)
	Remove(ctx context.Context, clusterID, sandboxID, executionID string) (bool, error)
}

type workloadCounter interface {
	Count(ctx context.Context, clusterID string) (uint64, error)
}

type workloadLease struct {
	store       workloadLeaseStore
	clusterID   string
	sandboxID   string
	executionID string
	leaseTTL    time.Duration
	ctx         context.Context //nolint:containedctx // the lease owns its heartbeat context
	cancel      context.CancelFunc
	heartbeatWG sync.WaitGroup

	errMu sync.Mutex
	err   error

	stopOnce   sync.Once
	removeOnce sync.Once
}

func (o *Orchestrator) recordWorkloadLifecycle(ctx context.Context, operation, outcome string) {
	if o.workloadLifecycleCounter == nil {
		return
	}

	o.workloadLifecycleCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("operation", operation),
		attribute.String("outcome", outcome),
		attribute.String("mode", string(o.capacityDemandMode)),
	))
}

func beginWorkloadLease(
	ctx context.Context,
	store workloadLeaseStore,
	clusterID, sandboxID, executionID string,
	leaseTTL, heartbeatInterval time.Duration,
) (*workloadLease, error) {
	if store == nil {
		return nil, errors.New("workload store is required")
	}
	if leaseTTL <= 0 || heartbeatInterval <= 0 {
		return nil, errors.New("workload lease durations must be positive")
	}

	now := time.Now().UTC()
	created, err := store.Acquire(ctx, clusterID, sandboxID, executionID, now, now.Add(leaseTTL))
	if err != nil {
		return nil, fmt.Errorf("persist workload lease: %w", err)
	}
	if !created {
		return nil, errors.New("persist workload lease: sandbox already has an active execution")
	}

	leaseCtx, cancel := context.WithCancel(ctx)
	lease := &workloadLease{
		store:       store,
		clusterID:   clusterID,
		sandboxID:   sandboxID,
		executionID: executionID,
		leaseTTL:    leaseTTL,
		ctx:         leaseCtx,
		cancel:      cancel,
	}
	lease.heartbeatWG.Add(1)
	go lease.heartbeat(heartbeatInterval)

	return lease, nil
}

func (l *workloadLease) heartbeat(interval time.Duration) {
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
		updated, err := l.store.Renew(l.ctx, l.clusterID, l.sandboxID, l.executionID, now, now.Add(l.leaseTTL))
		if l.ctx.Err() != nil {
			return
		}
		if err == nil && updated {
			continue
		}
		if err == nil {
			err = errors.New("workload lease is no longer owned by this execution")
		}
		l.setError(fmt.Errorf("refresh workload lease: %w", err))
		l.cancel()

		return
	}
}

func (l *workloadLease) Context() context.Context {
	return l.ctx
}

func (l *workloadLease) Err() error {
	l.errMu.Lock()
	defer l.errMu.Unlock()

	return l.err
}

func (l *workloadLease) setError(err error) {
	l.errMu.Lock()
	defer l.errMu.Unlock()
	if l.err == nil {
		l.err = err
	}
}

func (l *workloadLease) stopHeartbeat() {
	l.stopOnce.Do(func() {
		l.cancel()
		l.heartbeatWG.Wait()
	})
}

func (l *workloadLease) PrepareCommit(requestCtx context.Context, endTime time.Time) error {
	l.stopHeartbeat()
	if err := l.Err(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(requestCtx), workloadMutationTimeout)
	defer cancel()
	now := time.Now().UTC()
	updated, err := l.store.Renew(ctx, l.clusterID, l.sandboxID, l.executionID, now, endTime)
	if err != nil {
		return fmt.Errorf("prepare workload commit: %w", err)
	}
	if !updated {
		return errors.New("prepare workload commit: lease is no longer owned by this execution")
	}

	return nil
}

func (l *workloadLease) Promote(requestCtx context.Context, endTime time.Time) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(requestCtx), workloadMutationTimeout)
	defer cancel()
	now := time.Now().UTC()
	updated, err := l.store.Promote(ctx, l.clusterID, l.sandboxID, l.executionID, now, endTime)
	if err != nil {
		return fmt.Errorf("promote workload: %w", err)
	}
	if !updated {
		return errors.New("promote workload: lease is no longer owned by this execution")
	}

	return nil
}

func (l *workloadLease) Remove(requestCtx context.Context) {
	l.stopHeartbeat()
	l.removeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(requestCtx), workloadMutationTimeout)
		defer cancel()
		if _, err := l.store.Remove(ctx, l.clusterID, l.sandboxID, l.executionID); err != nil {
			logger.L().Error(requestCtx, "failed to remove workload lease",
				zap.Error(err),
				zap.String("cluster_id", l.clusterID),
				logger.WithSandboxID(l.sandboxID),
			)
		}
	})
}
