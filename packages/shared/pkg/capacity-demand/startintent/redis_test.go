package startintent

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	redisutils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

func TestRedisStoreUpsertIsIdempotentAndOwnerScoped(t *testing.T) { //nolint:paralleltest // tests may share the configured Redis instance
	client := setupRedis(t)
	store := NewRedisStore(client)
	now := time.Date(2026, time.August, 26, 10, 0, 0, 0, time.UTC)
	original := Intent{
		ClusterID:     "cluster-a",
		SandboxID:     "sandbox-1",
		OwnerToken:    "owner-1",
		VCPU:          2,
		MemoryMiB:     4096,
		Compatibility: "client-pool-amd64",
	}

	created, err := store.Upsert(t.Context(), original, now, now.Add(time.Minute))
	require.NoError(t, err)
	require.True(t, created)

	changed := original
	changed.VCPU = 8
	changed.MemoryMiB = 16384
	changed.Compatibility = "different-pool"
	created, err = store.Upsert(t.Context(), changed, now.Add(time.Second), now.Add(2*time.Minute))
	require.NoError(t, err)
	require.False(t, created)

	records, err := store.Active(t.Context(), original.ClusterID, now.Add(2*time.Second))
	require.NoError(t, err)
	require.Equal(t, []Record{{
		Intent:        original,
		SchemaVersion: SchemaVersion,
		State:         StateOutstanding,
		CreatedAt:     now,
		ExpiresAt:     now.Add(time.Minute),
	}}, records)

	replacement := original
	replacement.OwnerToken = "owner-2"
	created, err = store.Upsert(t.Context(), replacement, now.Add(3*time.Second), now.Add(2*time.Minute))
	require.NoError(t, err)
	require.False(t, created)

	records, err = store.Active(t.Context(), original.ClusterID, now.Add(4*time.Second))
	require.NoError(t, err)
	require.Equal(t, original.OwnerToken, records[0].OwnerToken)
}

func TestRedisStoreExpiredLeaseCanBeReplacedWithoutOldOwnerMutation(t *testing.T) { //nolint:paralleltest // tests may share the configured Redis instance
	client := setupRedis(t)
	store := NewRedisStore(client)
	now := time.Date(2026, time.August, 26, 10, 0, 0, 0, time.UTC)
	oldIntent := validIntent()
	requireCreated(t, store, oldIntent, now, now.Add(time.Second))

	newIntent := oldIntent
	newIntent.OwnerToken = "new-owner"
	created, err := store.Upsert(t.Context(), newIntent, now.Add(2*time.Second), now.Add(time.Minute))
	require.NoError(t, err)
	require.True(t, created)

	updated, err := store.Heartbeat(t.Context(), oldIntent.ClusterID, oldIntent.SandboxID, oldIntent.OwnerToken, now.Add(3*time.Second), now.Add(2*time.Minute))
	require.NoError(t, err)
	require.False(t, updated)
	transitioned, err := store.Handoff(t.Context(), oldIntent.ClusterID, oldIntent.SandboxID, oldIntent.OwnerToken, now.Add(3*time.Second), now.Add(30*time.Second))
	require.NoError(t, err)
	require.False(t, transitioned)
	removed, err := store.Remove(t.Context(), oldIntent.ClusterID, oldIntent.SandboxID, oldIntent.OwnerToken)
	require.NoError(t, err)
	require.False(t, removed)

	records, err := store.Active(t.Context(), newIntent.ClusterID, now.Add(4*time.Second))
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, newIntent.OwnerToken, records[0].OwnerToken)
	require.Equal(t, StateOutstanding, records[0].State)
}

func TestRedisStoreHeartbeatOnlyRefreshesMatchingOutstandingRecord(t *testing.T) { //nolint:paralleltest // tests may share the configured Redis instance
	client := setupRedis(t)
	store := NewRedisStore(client)
	now := time.Date(2026, time.August, 26, 10, 0, 0, 0, time.UTC)
	intent := validIntent()
	requireCreated(t, store, intent, now, now.Add(time.Second))

	updated, err := store.Heartbeat(t.Context(), intent.ClusterID, intent.SandboxID, intent.OwnerToken, now.Add(500*time.Millisecond), now.Add(time.Minute))
	require.NoError(t, err)
	require.True(t, updated)
	updated, err = store.Heartbeat(t.Context(), intent.ClusterID, "missing", intent.OwnerToken, now, now.Add(time.Minute))
	require.NoError(t, err)
	require.False(t, updated)

	records, err := store.Active(t.Context(), intent.ClusterID, now.Add(2*time.Second))
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, now.Add(time.Minute), records[0].ExpiresAt)

	ids, err := store.ActiveIDs(t.Context(), intent.ClusterID, now.Add(2*time.Second))
	require.NoError(t, err)
	require.Equal(t, []string{intent.SandboxID}, ids)
}

func TestRedisStoreHandoffOnlyTransitionsMatchingOutstandingRecord(t *testing.T) { //nolint:paralleltest // tests may share the configured Redis instance
	client := setupRedis(t)
	store := NewRedisStore(client)
	now := time.Date(2026, time.August, 26, 10, 0, 0, 0, time.UTC)
	intent := validIntent()
	requireCreated(t, store, intent, now, now.Add(time.Minute))

	transitioned, err := store.Handoff(t.Context(), intent.ClusterID, intent.SandboxID, "wrong-owner", now.Add(time.Second), now.Add(30*time.Second))
	require.NoError(t, err)
	require.False(t, transitioned)
	transitioned, err = store.Handoff(t.Context(), intent.ClusterID, intent.SandboxID, intent.OwnerToken, now.Add(time.Second), now.Add(30*time.Second))
	require.NoError(t, err)
	require.True(t, transitioned)
	transitioned, err = store.Handoff(t.Context(), intent.ClusterID, intent.SandboxID, intent.OwnerToken, now.Add(2*time.Second), now.Add(40*time.Second))
	require.NoError(t, err)
	require.False(t, transitioned)

	updated, err := store.Heartbeat(t.Context(), intent.ClusterID, intent.SandboxID, intent.OwnerToken, now.Add(2*time.Second), now.Add(time.Minute))
	require.NoError(t, err)
	require.False(t, updated)
	records, err := store.Active(t.Context(), intent.ClusterID, now.Add(2*time.Second))
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, StateHandoff, records[0].State)
	require.Equal(t, now.Add(30*time.Second), records[0].ExpiresAt)
}

func TestRedisStoreRemoveRequiresMatchingOwner(t *testing.T) { //nolint:paralleltest // tests may share the configured Redis instance
	client := setupRedis(t)
	store := NewRedisStore(client)
	now := time.Date(2026, time.August, 26, 10, 0, 0, 0, time.UTC)
	intent := validIntent()
	requireCreated(t, store, intent, now, now.Add(time.Minute))

	removed, err := store.Remove(t.Context(), intent.ClusterID, intent.SandboxID, "wrong-owner")
	require.NoError(t, err)
	require.False(t, removed)
	removed, err = store.Remove(t.Context(), intent.ClusterID, intent.SandboxID, intent.OwnerToken)
	require.NoError(t, err)
	require.True(t, removed)
	removed, err = store.Remove(t.Context(), intent.ClusterID, intent.SandboxID, intent.OwnerToken)
	require.NoError(t, err)
	require.False(t, removed)

	ids, err := store.ActiveIDs(t.Context(), intent.ClusterID, now)
	require.NoError(t, err)
	require.Empty(t, ids)
}

func TestRedisStoreActiveCleansExpiredRecordsAndIsClusterScoped(t *testing.T) { //nolint:paralleltest // tests may share the configured Redis instance
	client := setupRedis(t)
	store := NewRedisStore(client)
	now := time.Date(2026, time.August, 26, 10, 0, 0, 0, time.UTC)

	expired := validIntent()
	expired.SandboxID = "expired"
	requireCreated(t, store, expired, now, now.Add(time.Second))
	active := validIntent()
	active.SandboxID = "active-b"
	requireCreated(t, store, active, now, now.Add(time.Minute))
	active.SandboxID = "active-a"
	requireCreated(t, store, active, now, now.Add(time.Minute))
	other := validIntent()
	other.ClusterID = "cluster-b"
	other.SandboxID = "other"
	requireCreated(t, store, other, now, now.Add(time.Minute))

	ids, err := store.ActiveIDs(t.Context(), "cluster-a", now.Add(2*time.Second))
	require.NoError(t, err)
	require.Equal(t, []string{"active-a", "active-b"}, ids)
	require.False(t, client.HExists(t.Context(), entriesKey("cluster-a"), expired.SandboxID).Val())
	require.ErrorIs(t, client.ZScore(t.Context(), deadlinesKey("cluster-a"), expired.SandboxID).Err(), redis.Nil)
}

func TestRedisStoreRejectsInvalidInputsAndCorruptRecords(t *testing.T) { //nolint:paralleltest // tests may share the configured Redis instance
	client := setupRedis(t)
	store := NewRedisStore(client)
	now := time.Date(2026, time.August, 26, 10, 0, 0, 0, time.UTC)
	valid := validIntent()

	tests := map[string]Intent{
		"cluster missing":       withIntent(valid, func(i *Intent) { i.ClusterID = "" }),
		"cluster braces":        withIntent(valid, func(i *Intent) { i.ClusterID = "bad{cluster}" }),
		"cluster too long":      withIntent(valid, func(i *Intent) { i.ClusterID = strings.Repeat("a", maxClusterIDLen+1) }),
		"sandbox missing":       withIntent(valid, func(i *Intent) { i.SandboxID = "" }),
		"sandbox too long":      withIntent(valid, func(i *Intent) { i.SandboxID = strings.Repeat("a", maxSandboxIDLen+1) }),
		"owner missing":         withIntent(valid, func(i *Intent) { i.OwnerToken = "" }),
		"owner too long":        withIntent(valid, func(i *Intent) { i.OwnerToken = strings.Repeat("a", maxOwnerTokenLen+1) }),
		"vcpu invalid":          withIntent(valid, func(i *Intent) { i.VCPU = 0 }),
		"memory invalid":        withIntent(valid, func(i *Intent) { i.MemoryMiB = 0 }),
		"compatibility missing": withIntent(valid, func(i *Intent) { i.Compatibility = "" }),
		"compatibility too long": withIntent(valid, func(i *Intent) {
			i.Compatibility = strings.Repeat("a", maxCompatibilityLen+1)
		}),
	}
	for name, intent := range tests { //nolint:paralleltest // subtests share one Redis database
		t.Run(name, func(t *testing.T) {
			_, err := store.Upsert(t.Context(), intent, now, now.Add(time.Minute))
			require.Error(t, err)
		})
	}

	_, err := store.Upsert(t.Context(), valid, time.Time{}, now.Add(time.Minute))
	require.Error(t, err)
	_, err = store.Upsert(t.Context(), valid, now, now)
	require.Error(t, err)
	_, err = store.Heartbeat(t.Context(), valid.ClusterID, valid.SandboxID, "", now, now.Add(time.Minute))
	require.Error(t, err)
	_, err = store.Handoff(t.Context(), valid.ClusterID, valid.SandboxID, valid.OwnerToken, now, now)
	require.Error(t, err)
	_, err = store.Remove(t.Context(), valid.ClusterID, valid.SandboxID, "")
	require.Error(t, err)
	_, err = store.Active(t.Context(), "", now)
	require.Error(t, err)

	require.NoError(t, client.HSet(t.Context(), entriesKey(valid.ClusterID), valid.SandboxID, "not-json").Err())
	require.NoError(t, client.ZAdd(t.Context(), deadlinesKey(valid.ClusterID), redis.Z{
		Score:  float64(now.Add(time.Minute).UnixMilli()),
		Member: valid.SandboxID,
	}).Err())
	_, err = store.Active(t.Context(), valid.ClusterID, now)
	require.Error(t, err)
}

func TestRedisStoreConcurrentUpsertKeepsOneRecord(t *testing.T) { //nolint:paralleltest // tests may share the configured Redis instance
	client := setupRedis(t)
	store := NewRedisStore(client)
	now := time.Date(2026, time.August, 26, 10, 0, 0, 0, time.UTC)
	intent := validIntent()
	var created atomic.Int64
	var wg sync.WaitGroup
	errors := make(chan error, 32)

	for range 32 {
		wg.Go(func() {
			wasCreated, err := store.Upsert(t.Context(), intent, now, now.Add(time.Minute))
			if err != nil {
				errors <- err

				return
			}
			if wasCreated {
				created.Add(1)
			}
		})
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	require.Equal(t, int64(1), created.Load())

	records, err := store.Active(t.Context(), intent.ClusterID, now)
	require.NoError(t, err)
	require.Len(t, records, 1)
}

func TestRedisStoreUsesVersionedNamespaceSeparateFromLegacy(t *testing.T) {
	t.Parallel()

	require.Equal(t, "e2b:capacity-demand:start-intent:v1:{cluster-a}:entries", entriesKey("cluster-a"))
	require.Equal(t, "e2b:capacity-demand:start-intent:v1:{cluster-a}:deadlines", deadlinesKey("cluster-a"))
}

func TestRedisStoreActiveCleansLargeExpiredBurstInBoundedBatches(t *testing.T) { //nolint:paralleltest // tests may share the configured Redis instance
	client := setupRedis(t)
	store := NewRedisStore(client)
	clusterID := "cluster-large-expiry"
	now := time.Date(2026, time.August, 26, 10, 0, 0, 0, time.UTC)

	pipe := client.Pipeline()
	for i := range 10_000 {
		sandboxID := fmt.Sprintf("expired-%05d", i)
		pipe.HSet(t.Context(), entriesKey(clusterID), sandboxID, `{}`)
		pipe.ZAdd(t.Context(), deadlinesKey(clusterID), redis.Z{
			Score:  float64(now.Add(-time.Second).UnixMilli()),
			Member: sandboxID,
		})
	}
	_, err := pipe.Exec(t.Context())
	require.NoError(t, err)

	records, err := store.Active(t.Context(), clusterID, now)
	require.NoError(t, err)
	require.Empty(t, records)
	require.Zero(t, client.HLen(t.Context(), entriesKey(clusterID)).Val())
	require.Zero(t, client.ZCard(t.Context(), deadlinesKey(clusterID)).Val())
}

func TestRedisStoreRejectsMissingClient(t *testing.T) {
	t.Parallel()

	var store *RedisStore
	now := time.Date(2026, time.August, 26, 10, 0, 0, 0, time.UTC)
	intent := validIntent()
	_, err := store.Upsert(t.Context(), intent, now, now.Add(time.Minute))
	require.Error(t, err)
	_, err = store.Heartbeat(t.Context(), intent.ClusterID, intent.SandboxID, intent.OwnerToken, now, now.Add(time.Minute))
	require.Error(t, err)
	_, err = store.Handoff(t.Context(), intent.ClusterID, intent.SandboxID, intent.OwnerToken, now, now.Add(time.Minute))
	require.Error(t, err)
	_, err = store.Remove(t.Context(), intent.ClusterID, intent.SandboxID, intent.OwnerToken)
	require.Error(t, err)
	_, err = store.Active(t.Context(), intent.ClusterID, now)
	require.Error(t, err)
}

func validIntent() Intent {
	return Intent{
		ClusterID:     "cluster-a",
		SandboxID:     "sandbox-1",
		OwnerToken:    "owner-1",
		VCPU:          2,
		MemoryMiB:     4096,
		Compatibility: "client-pool-amd64",
	}
}

func withIntent(intent Intent, change func(*Intent)) Intent {
	change(&intent)

	return intent
}

func requireCreated(t *testing.T, store *RedisStore, intent Intent, now, expiresAt time.Time) {
	t.Helper()
	created, err := store.Upsert(t.Context(), intent, now, expiresAt)
	require.NoError(t, err)
	require.True(t, created)
}

func setupRedis(t *testing.T) redis.UniversalClient {
	t.Helper()

	address := os.Getenv("START_INTENT_TEST_REDIS_ADDR")
	if address == "" {
		return redisutils.SetupInstance(t)
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
