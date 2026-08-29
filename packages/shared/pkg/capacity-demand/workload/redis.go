package workload

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const keyPrefix = "e2b:capacity-demand:workload:v2"

type RedisStore struct {
	client   redis.UniversalClient
	clusters sync.Map
}

func NewRedisStore(client redis.UniversalClient) *RedisStore {
	return &RedisStore{client: client}
}

func (s *RedisStore) Acquire(ctx context.Context, clusterID, sandboxID, executionID string, now, expiresAt time.Time) (bool, error) {
	if err := validateMutation(clusterID, sandboxID, executionID, now, expiresAt); err != nil {
		return false, err
	}
	if err := s.validateClient(); err != nil {
		return false, err
	}

	result, err := acquireScript.Run(ctx, s.client, s.keys(clusterID), sandboxID, executionID, expiresAt.UnixMilli()).Int64()
	if err != nil {
		return false, fmt.Errorf("acquire workload: %w", err)
	}
	s.track(clusterID)

	return result == 1, nil
}

func (s *RedisStore) Renew(ctx context.Context, clusterID, sandboxID, executionID string, now, expiresAt time.Time) (bool, error) {
	return s.transition(ctx, clusterID, sandboxID, executionID, now, expiresAt, StateStarting, StateStarting)
}

func (s *RedisStore) Promote(ctx context.Context, clusterID, sandboxID, executionID string, now, expiresAt time.Time) (bool, error) {
	return s.transition(ctx, clusterID, sandboxID, executionID, now, expiresAt, StateStarting, StateRunning)
}

func (s *RedisStore) UpdateDeadline(ctx context.Context, clusterID, sandboxID, executionID string, now, expiresAt time.Time) (bool, error) {
	return s.transition(ctx, clusterID, sandboxID, executionID, now, expiresAt, StateRunning, StateRunning)
}

func (s *RedisStore) transition(ctx context.Context, clusterID, sandboxID, executionID string, now, expiresAt time.Time, expected, next State) (bool, error) {
	if err := validateMutation(clusterID, sandboxID, executionID, now, expiresAt); err != nil {
		return false, err
	}
	if err := s.validateClient(); err != nil {
		return false, err
	}

	result, err := transitionScript.Run(
		ctx,
		s.client,
		s.keys(clusterID),
		sandboxID,
		executionID,
		expiresAt.UnixMilli(),
		string(expected),
		string(next),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("transition workload from %s to %s: %w", expected, next, err)
	}
	s.track(clusterID)
	if result == -1 {
		return false, fmt.Errorf("%w: %s to %s", ErrInvalidTransition, expected, next)
	}

	return result == 1, nil
}

func (s *RedisStore) Remove(ctx context.Context, clusterID, sandboxID, executionID string) (bool, error) {
	if err := validateIdentity(clusterID, sandboxID, executionID); err != nil {
		return false, err
	}
	if err := s.validateClient(); err != nil {
		return false, err
	}

	result, err := removeScript.Run(ctx, s.client, s.keys(clusterID), sandboxID, executionID).Int64()
	if err != nil {
		return false, fmt.Errorf("remove workload: %w", err)
	}
	s.track(clusterID)

	return result == 1, nil
}

func (s *RedisStore) Count(ctx context.Context, clusterID string) (uint64, error) {
	if err := validateClusterID(clusterID); err != nil {
		return 0, err
	}
	if err := s.validateClient(); err != nil {
		return 0, err
	}

	result, err := countScript.Run(ctx, s.client, []string{deadlinesKey(clusterID)}).Uint64()
	if err != nil {
		return 0, fmt.Errorf("count active workloads: %w", err)
	}
	s.track(clusterID)

	return result, nil
}

func (s *RedisStore) SweepExpired(ctx context.Context, clusterID string, limit int64) (int64, error) {
	if err := validateClusterID(clusterID); err != nil {
		return 0, err
	}
	if limit <= 0 || limit > maxSweepBatch {
		return 0, fmt.Errorf("workload sweep limit must be between 1 and %d", maxSweepBatch)
	}
	if err := s.validateClient(); err != nil {
		return 0, err
	}

	result, err := sweepScript.Run(ctx, s.client, s.keys(clusterID), strconv.FormatInt(limit, 10)).Int64()
	if err != nil {
		return 0, fmt.Errorf("sweep expired workloads: %w", err)
	}
	s.track(clusterID)

	return result, nil
}

// RunSweeper periodically removes one bounded batch for every cluster seen by
// this process. Errors are reported and retried on the next tick.
func (s *RedisStore) RunSweeper(ctx context.Context, interval time.Duration, limit int64, report func(clusterID string, removed int64, err error)) error {
	if interval <= 0 {
		return errors.New("workload sweep interval must be positive")
	}
	if limit <= 0 || limit > maxSweepBatch {
		return fmt.Errorf("workload sweep limit must be between 1 and %d", maxSweepBatch)
	}
	if err := s.validateClient(); err != nil {
		return err
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}

		s.clusters.Range(func(key, _ any) bool {
			clusterID, ok := key.(string)
			if !ok {
				return true
			}
			removed, err := s.SweepExpired(ctx, clusterID, limit)
			if report != nil {
				report(clusterID, removed, err)
			}

			return ctx.Err() == nil
		})
	}
}

func (s *RedisStore) validateClient() error {
	if s == nil || s.client == nil {
		return errors.New("workload Redis client is required")
	}

	return nil
}

func (s *RedisStore) track(clusterID string) {
	s.clusters.Store(clusterID, struct{}{})
}

func (s *RedisStore) keys(clusterID string) []string {
	return []string{entriesKey(clusterID), deadlinesKey(clusterID)}
}

func entriesKey(clusterID string) string {
	return keyPrefix + ":{" + clusterID + "}:entries"
}

func deadlinesKey(clusterID string) string {
	return keyPrefix + ":{" + clusterID + "}:deadlines"
}
