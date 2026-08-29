package workload

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestRedisStoreLifecycleIsExecutionFenced(t *testing.T) {
	client := setupRedis(t)
	store := NewRedisStore(client)
	now := time.Now().UTC()

	created, err := store.Acquire(t.Context(), "cluster-a", "sandbox-a", "execution-a", now, now.Add(time.Minute))
	require.NoError(t, err)
	require.True(t, created)

	created, err = store.Acquire(t.Context(), "cluster-a", "sandbox-a", "execution-a", now, now.Add(2*time.Minute))
	require.NoError(t, err)
	require.False(t, created)

	created, err = store.Acquire(t.Context(), "cluster-a", "sandbox-a", "execution-b", now, now.Add(2*time.Minute))
	require.NoError(t, err)
	require.False(t, created)

	updated, err := store.Renew(t.Context(), "cluster-a", "sandbox-a", "execution-b", now, now.Add(2*time.Minute))
	require.NoError(t, err)
	require.False(t, updated)

	updated, err = store.Renew(t.Context(), "cluster-a", "sandbox-a", "execution-a", now, now.Add(2*time.Minute))
	require.NoError(t, err)
	require.True(t, updated)

	updated, err = store.Promote(t.Context(), "cluster-a", "sandbox-a", "execution-a", now, now.Add(2*time.Minute))
	require.NoError(t, err)
	require.True(t, updated)

	updated, err = store.Renew(t.Context(), "cluster-a", "sandbox-a", "execution-a", now, now.Add(3*time.Minute))
	require.ErrorIs(t, err, ErrInvalidTransition)
	require.False(t, updated)

	updated, err = store.UpdateDeadline(t.Context(), "cluster-a", "sandbox-a", "execution-a", now, now.Add(3*time.Minute))
	require.NoError(t, err)
	require.True(t, updated)

	removed, err := store.Remove(t.Context(), "cluster-a", "sandbox-a", "execution-b")
	require.NoError(t, err)
	require.False(t, removed)

	removed, err = store.Remove(t.Context(), "cluster-a", "sandbox-a", "execution-a")
	require.NoError(t, err)
	require.True(t, removed)

	count, err := store.Count(t.Context(), "cluster-a")
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestRedisStoreExpiredExecutionCanBeReplacedWithoutABA(t *testing.T) {
	client := setupRedis(t)
	store := NewRedisStore(client)
	now := time.Now().UTC()

	expiredAt := now.Add(-time.Minute).UnixMilli()
	require.NoError(t, client.HSet(t.Context(), entriesKey("cluster-a"), "sandbox-a", fmt.Sprintf(`{"schema_version":2,"execution_id":"execution-old","state":"running","expires_at_ms":%d}`, expiredAt)).Err())
	require.NoError(t, client.ZAdd(t.Context(), deadlinesKey("cluster-a"), redis.Z{Member: "sandbox-a", Score: float64(expiredAt)}).Err())

	created, err := store.Acquire(t.Context(), "cluster-a", "sandbox-a", "execution-new", now, now.Add(time.Minute))
	require.NoError(t, err)
	require.True(t, created)

	removed, err := store.Remove(t.Context(), "cluster-a", "sandbox-a", "execution-old")
	require.NoError(t, err)
	require.False(t, removed)

	updated, err := store.UpdateDeadline(t.Context(), "cluster-a", "sandbox-a", "execution-old", now, now.Add(2*time.Minute))
	require.NoError(t, err)
	require.False(t, updated)

	count, err := store.Count(t.Context(), "cluster-a")
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)
}

func TestRedisStoreUsesRedisTimeForExpiryDecisions(t *testing.T) {
	client := setupRedis(t)
	store := NewRedisStore(client)
	now := time.Now().UTC()

	created, err := store.Acquire(t.Context(), "cluster-a", "sandbox-a", "execution-a", now, now.Add(time.Hour))
	require.NoError(t, err)
	require.True(t, created)

	created, err = store.Acquire(t.Context(), "cluster-a", "sandbox-a", "execution-b", now.Add(2*time.Hour), now.Add(3*time.Hour))
	require.NoError(t, err)
	require.False(t, created, "a fast API clock must not replace a lease that Redis still considers active")

	_, err = store.Acquire(t.Context(), "cluster-a", "sandbox-b", "execution-b", now.Add(-2*time.Hour), now.Add(-time.Hour))
	require.ErrorContains(t, err, "expiry must be in the future", "a slow API clock must not create a lease Redis already considers expired")
}

func TestRedisStoreCountUsesDeadlineWithoutReadingPayload(t *testing.T) {
	client := setupRedis(t)
	store := NewRedisStore(client)
	now := time.Now().UTC()

	require.NoError(t, client.HSet(t.Context(), entriesKey("cluster-a"), "active", "not-json").Err())
	require.NoError(t, client.ZAdd(t.Context(), deadlinesKey("cluster-a"), redis.Z{
		Member: "active",
		Score:  float64(now.Add(time.Minute).UnixMilli()),
	}).Err())
	require.NoError(t, client.HSet(t.Context(), entriesKey("cluster-a"), "expired", "not-json").Err())
	require.NoError(t, client.ZAdd(t.Context(), deadlinesKey("cluster-a"), redis.Z{
		Member: "expired",
		Score:  float64(now.Add(-time.Minute).UnixMilli()),
	}).Err())

	count, err := store.Count(t.Context(), "cluster-a")
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)
	require.True(t, client.HExists(t.Context(), entriesKey("cluster-a"), "expired").Val())
}

func TestRedisStoreSweepExpiredHasHardBatchLimitAndCleansBothIndexes(t *testing.T) {
	client := setupRedis(t)
	store := NewRedisStore(client)
	now := time.Now().UTC()

	pipe := client.Pipeline()
	for index := range maxSweepBatch + 10 {
		sandboxID := fmt.Sprintf("expired-%03d", index)
		pipe.HSet(t.Context(), entriesKey("cluster-a"), sandboxID, `{}`)
		pipe.ZAdd(t.Context(), deadlinesKey("cluster-a"), redis.Z{
			Member: sandboxID,
			Score:  float64(now.Add(-time.Minute).UnixMilli()),
		})
	}
	_, err := pipe.Exec(t.Context())
	require.NoError(t, err)

	removed, err := store.SweepExpired(t.Context(), "cluster-a", maxSweepBatch)
	require.NoError(t, err)
	require.Equal(t, int64(maxSweepBatch), removed)
	require.Equal(t, int64(10), client.HLen(t.Context(), entriesKey("cluster-a")).Val())
	require.Equal(t, int64(10), client.ZCard(t.Context(), deadlinesKey("cluster-a")).Val())

	removed, err = store.SweepExpired(t.Context(), "cluster-a", maxSweepBatch)
	require.NoError(t, err)
	require.Equal(t, int64(10), removed)
	require.Zero(t, client.HLen(t.Context(), entriesKey("cluster-a")).Val())
	require.Zero(t, client.ZCard(t.Context(), deadlinesKey("cluster-a")).Val())
}

func TestRedisStoreRunSweeperEventuallyConverges(t *testing.T) {
	client := setupRedis(t)
	store := NewRedisStore(client)
	now := time.Now().UTC()

	pipe := client.Pipeline()
	for index := range maxSweepBatch + 10 {
		sandboxID := fmt.Sprintf("expired-%03d", index)
		expiredAt := now.Add(-time.Minute).UnixMilli()
		pipe.HSet(t.Context(), entriesKey("cluster-a"), sandboxID, fmt.Sprintf(`{"schema_version":2,"execution_id":"execution","state":"running","expires_at_ms":%d}`, expiredAt))
		pipe.ZAdd(t.Context(), deadlinesKey("cluster-a"), redis.Z{Member: sandboxID, Score: float64(expiredAt)})
	}
	_, err := pipe.Exec(t.Context())
	require.NoError(t, err)
	_, err = store.Count(t.Context(), "cluster-a")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- store.RunSweeper(ctx, 5*time.Millisecond, maxSweepBatch, nil)
	}()
	require.Eventually(t, func() bool {
		return client.ZCard(t.Context(), deadlinesKey("cluster-a")).Val() == 0 &&
			client.HLen(t.Context(), entriesKey("cluster-a")).Val() == 0
	}, time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
}

func TestRedisStoreRejectsInvalidInputs(t *testing.T) {
	t.Parallel()

	store := NewRedisStore(nil)
	now := time.Now().UTC()
	_, err := store.Acquire(t.Context(), "", "sandbox", "execution", now, now.Add(time.Minute))
	require.Error(t, err)
	_, err = store.Acquire(t.Context(), "cluster", "", "execution", now, now.Add(time.Minute))
	require.Error(t, err)
	_, err = store.Acquire(t.Context(), "cluster", "sandbox", "", now, now.Add(time.Minute))
	require.Error(t, err)
	_, err = store.Acquire(t.Context(), "cluster", "sandbox", "execution", now, now)
	require.Error(t, err)
	_, err = store.SweepExpired(t.Context(), "cluster", 0)
	require.Error(t, err)
	_, err = store.SweepExpired(t.Context(), "cluster", maxSweepBatch+1)
	require.Error(t, err)
}

func TestRedisStoreUsesVersionedClusterScopedKeys(t *testing.T) {
	t.Parallel()

	require.Equal(t, "e2b:capacity-demand:workload:v2:{cluster-a}:entries", entriesKey("cluster-a"))
	require.Equal(t, "e2b:capacity-demand:workload:v2:{cluster-a}:deadlines", deadlinesKey("cluster-a"))
}

func setupRedis(t *testing.T) redis.UniversalClient {
	t.Helper()

	address := os.Getenv("WORKLOAD_TEST_REDIS_ADDR")
	if address == "" {
		t.Skip("WORKLOAD_TEST_REDIS_ADDR is required")
	}

	client := redis.NewClient(&redis.Options{Addr: address})
	require.NoError(t, client.Ping(t.Context()).Err())
	require.NoError(t, client.FlushDB(t.Context()).Err())
	t.Cleanup(func() {
		require.NoError(t, client.FlushDB(context.WithoutCancel(t.Context())).Err())
		require.NoError(t, client.Close())
	})

	return client
}
