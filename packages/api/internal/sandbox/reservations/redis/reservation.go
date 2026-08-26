package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	resultTTL = 30 * time.Second

	// fallbackPollInterval is how often the waiter re-checks Redis when no
	// PubSub wakeup arrives
	fallbackPollInterval = 1 * time.Second

	// staleTTL is the maximum age of a pending entry without an owner heartbeat
	// before it is considered stale and cleaned up. This handles an API instance
	// crashing mid-creation without limiting legitimate capacity waits.
	staleTTL = 90 * time.Second

	// reservationHeartbeatInterval keeps an active owner's reservation younger
	// than staleTTL while a start waits for infrastructure capacity.
	reservationHeartbeatInterval = staleTTL / 3
)

var _ sandboxtypes.ReservationStorage = (*ReservationStorage)(nil)

// Publish is fire-and-forget — drops on queue saturation are recovered by the waiter's fallback ticker.
type Notifier interface {
	Subscribe(routingKey string) (<-chan struct{}, func())
	Publish(ctx context.Context, routingKey string)
}

type ReservationStorage struct {
	redisClient       redis.UniversalClient
	notifier          Notifier
	heartbeatInterval time.Duration
}

func NewReservationStorage(redisClient redis.UniversalClient, notifier Notifier) *ReservationStorage {
	return &ReservationStorage{
		redisClient:       redisClient,
		notifier:          notifier,
		heartbeatInterval: reservationHeartbeatInterval,
	}
}

func (s *ReservationStorage) Reserve(ctx context.Context, teamID uuid.UUID, sandboxID string, limit int) (finishStart func(sandboxtypes.Sandbox, error), waitForStart func(ctx context.Context) (sandboxtypes.Sandbox, error), err error) {
	teamIDStr := teamID.String()
	storageIndexKey := getStorageIndexKey(teamIDStr)
	pendingSetKey := getPendingSetKey(teamIDStr)
	resultKeyStr := getResultKey(teamIDStr, sandboxID)

	now := float64(time.Now().Unix())
	staleCutoff := float64(time.Now().Add(-staleTTL).Unix())

	result, err := reserveScript.Run(ctx, s.redisClient,
		[]string{storageIndexKey, pendingSetKey, resultKeyStr},
		sandboxID, limit, now, staleCutoff,
	).Int()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to run reserve script: %w", err)
	}

	switch result {
	case reserveResultReserved:
		stopHeartbeat := s.startReservationHeartbeat(ctx, teamID, sandboxID)
		finishStart := s.createFinishStart(ctx, teamID, sandboxID)

		return func(sbx sandboxtypes.Sandbox, startErr error) {
			// Wait until the owner heartbeat has stopped before removing the lease.
			// This ensures an old owner cannot refresh a later reservation that
			// reuses the same sandbox ID.
			stopHeartbeat()
			finishStart(sbx, startErr)
		}, nil, nil

	case reserveResultAlreadyInStorage:
		return nil, nil, sandboxtypes.ErrAlreadyExists

	case reserveResultAlreadyPending:
		return nil, s.createWaitForStart(teamID, sandboxID), nil

	case reserveResultLimitExceeded:
		return nil, nil, &sandboxtypes.LimitExceededError{TeamID: teamID}

	default:
		return nil, nil, fmt.Errorf("unexpected reserve script result: %d", result)
	}
}

// startReservationHeartbeat refreshes an active owner's lease until creation
// finishes or the owner context is cancelled. The Redis script is deliberately
// update-only: a heartbeat can never resurrect a reservation removed by a
// finish, release, or stale cleanup.
func (s *ReservationStorage) startReservationHeartbeat(ctx context.Context, teamID uuid.UUID, sandboxID string) func() {
	heartbeatCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)

		ticker := time.NewTicker(s.heartbeatInterval)
		defer ticker.Stop()

		pendingSetKey := getPendingSetKey(teamID.String())
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case now := <-ticker.C:
				refreshed, err := heartbeatScript.Run(
					heartbeatCtx,
					s.redisClient,
					[]string{pendingSetKey},
					sandboxID,
					float64(now.Unix()),
				).Int()
				if err != nil {
					if heartbeatCtx.Err() == nil {
						logger.L().Error(ctx, "failed to refresh sandbox reservation heartbeat",
							zap.Error(err),
							logger.WithSandboxID(sandboxID),
						)
					}

					continue
				}
				if refreshed == 0 {
					return
				}
			}
		}
	}()

	var stopOnce sync.Once

	return func() {
		stopOnce.Do(func() {
			cancel()
			<-done
		})
	}
}

func (s *ReservationStorage) Release(ctx context.Context, teamID uuid.UUID, sandboxID string) error {
	teamIDStr := teamID.String()
	pendingSetKey := getPendingSetKey(teamIDStr)
	resultKeyStr := getResultKey(teamIDStr, sandboxID)

	err := releaseScript.Run(ctx, s.redisClient,
		[]string{pendingSetKey, resultKeyStr},
		sandboxID,
	).Err()
	if err != nil {
		return fmt.Errorf("failed to run release script: %w", err)
	}

	// Wake any in-process waiter so it checks the pending set immediately.
	s.notifier.Publish(ctx, getReservationRoutingKey(teamIDStr, sandboxID))

	return nil
}

// createFinishStart returns a callback that completes the reservation.
func (s *ReservationStorage) createFinishStart(ctx context.Context, teamID uuid.UUID, sandboxID string) func(sandboxtypes.Sandbox, error) {
	return func(sbx sandboxtypes.Sandbox, startErr error) {
		teamIDStr := teamID.String()
		pendingSetKey := getPendingSetKey(teamIDStr)
		resultKeyStr := getResultKey(teamIDStr, sandboxID)
		routingKey := getReservationRoutingKey(teamIDStr, sandboxID)

		bgCtx := context.WithoutCancel(ctx)

		resultData, encodeErr := encodeResult(sbx, startErr)
		if encodeErr != nil {
			logger.L().Error(ctx, "failed to encode reservation result",
				zap.Error(encodeErr),
				logger.WithSandboxID(sandboxID),
			)

			// Still try to remove from pending even if encoding fails.
			_ = s.redisClient.ZRem(bgCtx, pendingSetKey, sandboxID).Err()

			// Wake waiters so they can observe that the reservation is gone.
			s.notifier.Publish(bgCtx, routingKey)

			return
		}

		ttlSeconds := int(resultTTL.Seconds())
		err := finishStartScript.Run(bgCtx, s.redisClient,
			[]string{pendingSetKey, resultKeyStr},
			sandboxID, resultData, ttlSeconds,
		).Err()
		if err != nil {
			logger.L().Error(ctx, "failed to run finishStart script",
				zap.Error(err),
				logger.WithSandboxID(sandboxID),
			)

			return
		}

		// Wake any in-process waiter immediately. Drop-tolerant: the
		// fallback ticker covers a saturated publish queue.
		s.notifier.Publish(bgCtx, routingKey)
	}
}

// createWaitForStart returns a function that polls Redis for the result of a sandbox creation
// initiated by another instance.
func (s *ReservationStorage) createWaitForStart(teamID uuid.UUID, sandboxID string) func(ctx context.Context) (sandboxtypes.Sandbox, error) {
	return func(ctx context.Context) (sandboxtypes.Sandbox, error) {
		teamIDStr := teamID.String()
		resultKeyStr := getResultKey(teamIDStr, sandboxID)
		pendingSetKey := getPendingSetKey(teamIDStr)
		routingKey := getReservationRoutingKey(teamIDStr, sandboxID)

		ch, cleanup := s.notifier.Subscribe(routingKey)
		defer cleanup()

		// Initial probe: the producer may have finished before we subscribed,
		// or we may be a late waiter joining after the result was already set.
		if done, sbx, err := s.tryReadResult(ctx, resultKeyStr, pendingSetKey, sandboxID); done {
			return sbx, err
		}

		ticker := time.NewTicker(fallbackPollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return sandboxtypes.Sandbox{}, ctx.Err()
			case <-ch:
			case <-ticker.C:
			}

			if done, sbx, err := s.tryReadResult(ctx, resultKeyStr, pendingSetKey, sandboxID); done {
				return sbx, err
			}
		}
	}
}

// tryReadResult performs a single probe of the reservation state.
//
// Returns done=true when the wait is over:
//   - the result key holds an encoded terminal result, or
//   - the sandbox vanished from the pending set without a result, or
//   - the Redis call itself failed.
//
// Returns done=false when the reservation is still pending and the caller
// should wait for the next wakeup.
func (s *ReservationStorage) tryReadResult(
	ctx context.Context,
	resultKey, pendingSetKey, sandboxID string,
) (done bool, sbx sandboxtypes.Sandbox, err error) {
	data, getErr := s.redisClient.Get(ctx, resultKey).Bytes()
	if getErr == nil {
		sbx, err = decodeResult(data)

		return true, sbx, err
	}
	if !errors.Is(getErr, redis.Nil) {
		return true, sandboxtypes.Sandbox{}, fmt.Errorf("failed to check result key: %w", getErr)
	}

	// No result yet, so check whether another instance is still creating the sandbox.
	scoreErr := s.redisClient.ZScore(ctx, pendingSetKey, sandboxID).Err()
	if errors.Is(scoreErr, redis.Nil) {
		// Re-read the result in case finishStart or a new Release wrote it
		// between the initial GET and the legacy pending-set check.
		data, getErr = s.redisClient.Get(ctx, resultKey).Bytes()
		if getErr == nil {
			sbx, err = decodeResult(data)

			return true, sbx, err
		}
		if !errors.Is(getErr, redis.Nil) {
			return true, sandboxtypes.Sandbox{}, fmt.Errorf("failed to check result key: %w", getErr)
		}

		return true, sandboxtypes.Sandbox{}, fmt.Errorf("sandbox %s is no longer pending and has no result", sandboxID)
	}
	if scoreErr != nil {
		return true, sandboxtypes.Sandbox{}, fmt.Errorf("failed to check pending set: %w", scoreErr)
	}

	// Still pending, no result yet.
	return false, sandboxtypes.Sandbox{}, nil
}
