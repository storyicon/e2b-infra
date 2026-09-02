package startintent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const keyPrefix = "e2b:capacity-demand:start-intent:v1"

type RedisStore struct {
	client redis.UniversalClient
}

// NewRedisStore creates a versioned start-intent store. It does not use the
// legacy pending/fulfilled capacity-demand namespace.
func NewRedisStore(client redis.UniversalClient) *RedisStore {
	return &RedisStore{client: client}
}

// Upsert creates an outstanding intent when no active lease exists. Repeated
// calls do not modify the existing owner, payload, state, or timestamps.
func (s *RedisStore) Upsert(ctx context.Context, intent Intent, now, expiresAt time.Time) (bool, error) {
	if err := validateIntent(intent); err != nil {
		return false, err
	}
	if err := validateLease(now, expiresAt); err != nil {
		return false, err
	}
	if err := s.validateClient(); err != nil {
		return false, err
	}

	payload, err := json.Marshal(newPersistedRecord(intent, now, expiresAt))
	if err != nil {
		return false, fmt.Errorf("encode start intent: %w", err)
	}
	result, err := upsertScript.Run(
		ctx,
		s.client,
		[]string{entriesKey(intent.ClusterID), deadlinesKey(intent.ClusterID)},
		intent.SandboxID,
		payload,
		expiresAt.UnixMilli(),
		now.UnixMilli(),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("upsert start intent: %w", err)
	}

	return result == 1, nil
}

// Heartbeat extends an unexpired outstanding lease owned by ownerToken.
func (s *RedisStore) Heartbeat(ctx context.Context, clusterID, sandboxID, ownerToken string, now, expiresAt time.Time) (bool, error) {
	if err := validateMutation(clusterID, sandboxID, ownerToken, now, expiresAt); err != nil {
		return false, err
	}
	if err := s.validateClient(); err != nil {
		return false, err
	}

	result, err := heartbeatScript.Run(
		ctx,
		s.client,
		[]string{entriesKey(clusterID), deadlinesKey(clusterID)},
		sandboxID,
		ownerToken,
		now.UnixMilli(),
		expiresAt.UnixMilli(),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("heartbeat start intent: %w", err)
	}

	return result == 1, nil
}

// Handoff atomically transitions an owned, unexpired lease from outstanding
// to handoff and applies the handoff deadline.
func (s *RedisStore) Handoff(ctx context.Context, clusterID, sandboxID, ownerToken string, now, expiresAt time.Time) (bool, error) {
	if err := validateMutation(clusterID, sandboxID, ownerToken, now, expiresAt); err != nil {
		return false, err
	}
	if err := s.validateClient(); err != nil {
		return false, err
	}

	result, err := handoffScript.Run(
		ctx,
		s.client,
		[]string{entriesKey(clusterID), deadlinesKey(clusterID)},
		sandboxID,
		ownerToken,
		now.UnixMilli(),
		expiresAt.UnixMilli(),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("handoff start intent: %w", err)
	}

	return result == 1, nil
}

// Remove deletes a lease only when ownerToken still owns the current record.
func (s *RedisStore) Remove(ctx context.Context, clusterID, sandboxID, ownerToken string) (bool, error) {
	if err := validateClusterID(clusterID); err != nil {
		return false, err
	}
	if err := validateIdentifier("sandbox ID", sandboxID, maxSandboxIDLen, false); err != nil {
		return false, err
	}
	if err := validateIdentifier("owner token", ownerToken, maxOwnerTokenLen, false); err != nil {
		return false, err
	}
	if err := s.validateClient(); err != nil {
		return false, err
	}

	result, err := removeScript.Run(
		ctx,
		s.client,
		[]string{entriesKey(clusterID), deadlinesKey(clusterID)},
		sandboxID,
		ownerToken,
	).Int64()
	if err != nil {
		return false, fmt.Errorf("remove start intent: %w", err)
	}

	return result == 1, nil
}

// Active removes expired leases and returns all remaining validated records
// for clusterID, ordered by sandbox ID.
func (s *RedisStore) Active(ctx context.Context, clusterID string, now time.Time) ([]Record, error) {
	if err := validateClusterID(clusterID); err != nil {
		return nil, err
	}
	if now.IsZero() {
		return nil, errors.New("start intent current time is required")
	}
	if err := s.validateClient(); err != nil {
		return nil, err
	}

	values, err := activeScript.Run(
		ctx,
		s.client,
		[]string{entriesKey(clusterID), deadlinesKey(clusterID)},
		now.UnixMilli(),
	).StringSlice()
	if err != nil {
		return nil, fmt.Errorf("read active start intents: %w", err)
	}
	if len(values)%3 != 0 {
		return nil, errors.New("read active start intents: invalid Redis response")
	}

	records := make([]Record, 0, len(values)/3)
	for index := 0; index < len(values); index += 3 {
		sandboxID := values[index]
		var persisted persistedRecord
		if err := json.Unmarshal([]byte(values[index+1]), &persisted); err != nil {
			return nil, fmt.Errorf("decode active start intent %q: %w", sandboxID, err)
		}
		deadlineMs, err := strconv.ParseInt(values[index+2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("decode active start intent %q deadline: %w", sandboxID, err)
		}
		if err := validateRecord(persisted, clusterID, sandboxID, now.UnixMilli(), deadlineMs); err != nil {
			return nil, fmt.Errorf("invalid active start intent %q: %w", sandboxID, err)
		}
		records = append(records, persisted.record())
	}
	slices.SortFunc(records, func(left, right Record) int {
		return strings.Compare(left.SandboxID, right.SandboxID)
	})

	return records, nil
}

// ActiveIDs returns the sandbox IDs represented by outstanding or handoff
// leases, ordered lexicographically.
func (s *RedisStore) ActiveIDs(ctx context.Context, clusterID string, now time.Time) ([]string, error) {
	records, err := s.Active(ctx, clusterID, now)
	if err != nil {
		return nil, err
	}

	ids := make([]string, 0, len(records))
	for _, record := range records {
		ids = append(ids, record.SandboxID)
	}

	return ids, nil
}

func (s *RedisStore) validateClient() error {
	if s == nil || s.client == nil {
		return errors.New("start intent Redis client is required")
	}

	return nil
}

func entriesKey(clusterID string) string {
	return keyPrefix + ":{" + clusterID + "}:entries"
}

func deadlinesKey(clusterID string) string {
	return keyPrefix + ":{" + clusterID + "}:deadlines"
}
