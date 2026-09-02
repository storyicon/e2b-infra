package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/placement"
	capacitydemand "github.com/e2b-dev/infra/packages/shared/pkg/capacity-demand"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	capacityDemandCleanupTimeout = 2 * time.Second
	capacityDemandExpiryGrace    = 30 * time.Second
)

type capacityDemandStore interface {
	Upsert(ctx context.Context, demand capacitydemand.Demand, expiresAt time.Time) error
	Fulfill(ctx context.Context, clusterID, sandboxID string) error
	RecordSuccess(ctx context.Context, clusterID string) error
	Remove(ctx context.Context, clusterID, sandboxID string) error
}

type capacityWaiter struct {
	store         capacityDemandStore
	timeout       time.Duration
	retryInterval time.Duration
}

type CapacityDemandStoreError struct {
	Err error
}

func (e CapacityDemandStoreError) Error() string {
	return fmt.Sprintf("capacity demand store unavailable: %v", e.Err)
}

func (e CapacityDemandStoreError) Unwrap() error {
	return e.Err
}

type CapacityWaitTimeoutError struct {
	Timeout time.Duration
}

func (e CapacityWaitTimeoutError) Error() string {
	return fmt.Sprintf("capacity did not become available within %s", e.Timeout)
}

func newCapacityWaiter(store capacityDemandStore, timeout, retryInterval time.Duration) *capacityWaiter {
	if store == nil || timeout <= 0 || retryInterval <= 0 {
		return nil
	}

	return &capacityWaiter{store: store, timeout: timeout, retryInterval: retryInterval}
}

func (w *capacityWaiter) Wait(ctx context.Context, demand capacitydemand.Demand, try func(context.Context) error) error {
	if w == nil {
		return try(ctx)
	}

	err := try(ctx)
	if err == nil {
		w.recordSuccess(ctx, demand)

		return nil
	}
	if !isCapacityUnavailable(err) {
		return err
	}

	waitCtx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()

	if err := w.store.Upsert(waitCtx, demand, time.Now().Add(w.timeout+capacityDemandExpiryGrace)); err != nil {
		return CapacityDemandStoreError{Err: err}
	}
	fulfilled := false
	defer func() { w.finishDemand(ctx, demand, fulfilled) }()

	ticker := time.NewTicker(w.retryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-waitCtx.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}

			return CapacityWaitTimeoutError{Timeout: w.timeout}
		case <-ticker.C:
		}

		err = try(waitCtx)
		if err == nil || !isCapacityUnavailable(err) {
			fulfilled = err == nil

			return err
		}
	}
}

// recordSuccess accounts for an owner request that used capacity without first
// entering the pending ledger. Redis only increments the burst counter while
// some demand for the cluster is still pending, so ordinary idle-cluster
// creates do not affect a future burst. The sandbox reservation guarantees one
// owner invocation; joined requests never enter this path.
func (w *capacityWaiter) recordSuccess(requestCtx context.Context, demand capacitydemand.Demand) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(requestCtx), capacityDemandCleanupTimeout)
	defer cancel()

	if err := w.store.RecordSuccess(cleanupCtx, demand.ClusterID); err != nil {
		logger.L().Error(requestCtx, "failed to record capacity success",
			zap.Error(err),
			zap.String("cluster_id", demand.ClusterID),
			logger.WithSandboxID(demand.SandboxID),
		)
	}
}

func (w *capacityWaiter) finishDemand(requestCtx context.Context, demand capacitydemand.Demand, fulfilled bool) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(requestCtx), capacityDemandCleanupTimeout)
	defer cancel()

	operation := "remove"
	finish := w.store.Remove
	if fulfilled {
		operation = "fulfill"
		finish = w.store.Fulfill
	}
	if err := finish(cleanupCtx, demand.ClusterID, demand.SandboxID); err != nil {
		logger.L().Error(requestCtx, "failed to finish capacity demand",
			zap.Error(err),
			zap.String("operation", operation),
			zap.String("cluster_id", demand.ClusterID),
			logger.WithSandboxID(demand.SandboxID),
		)
	}
}

func isCapacityUnavailable(err error) bool {
	var noNodes placement.NoNodesAvailableError
	if errors.As(err, &noNodes) {
		return true
	}

	var createErr placement.SandboxCreateError

	return errors.As(err, &createErr) && status.Code(createErr.LastErr) == codes.Unavailable
}
