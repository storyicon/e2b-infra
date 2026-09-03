package capacitydemand

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	redisutils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

func TestRedisStoreSummaryIsIdempotentAndClusterScoped(t *testing.T) {
	t.Parallel()

	client := redisutils.SetupInstance(t)
	store := NewRedisStore(client)
	now := time.Now().UTC()

	demand := Demand{
		ClusterID: "cluster-a",
		SandboxID: "sandbox-1",
		VCPU:      1,
		MemoryMiB: 4096,
	}
	require.NoError(t, store.Upsert(t.Context(), demand, now.Add(time.Minute)))
	require.NoError(t, store.Upsert(t.Context(), demand, now.Add(2*time.Minute)))
	require.NoError(t, store.Upsert(t.Context(), Demand{
		ClusterID: "cluster-a",
		SandboxID: "sandbox-2",
		VCPU:      2,
		MemoryMiB: 8192,
	}, now.Add(time.Minute)))
	require.NoError(t, store.Upsert(t.Context(), Demand{
		ClusterID: "cluster-b",
		SandboxID: "sandbox-3",
		VCPU:      8,
		MemoryMiB: 32768,
	}, now.Add(time.Minute)))

	summary, err := store.Summary(t.Context(), "cluster-a", now)
	require.NoError(t, err)
	require.Equal(t, Summary{Count: 2, VCPU: 3, MemoryMiB: 12288}, summary)
}

func TestRedisStoreRemovesAndExpiresDemand(t *testing.T) {
	t.Parallel()

	client := redisutils.SetupInstance(t)
	store := NewRedisStore(client)
	now := time.Now().UTC()

	require.NoError(t, store.Upsert(t.Context(), Demand{
		ClusterID: "cluster-a",
		SandboxID: "removed",
		VCPU:      1,
		MemoryMiB: 4096,
	}, now.Add(time.Minute)))
	require.NoError(t, store.Upsert(t.Context(), Demand{
		ClusterID: "cluster-a",
		SandboxID: "expired",
		VCPU:      1,
		MemoryMiB: 4096,
	}, now.Add(time.Second)))

	require.NoError(t, store.Remove(t.Context(), "cluster-a", "removed"))
	summary, err := store.Summary(t.Context(), "cluster-a", now.Add(2*time.Second))
	require.NoError(t, err)
	require.Equal(t, Summary{}, summary)
}

func TestRedisStoreFulfillsDemandExactlyOnce(t *testing.T) {
	t.Parallel()

	client := redisutils.SetupInstance(t)
	store := NewRedisStore(client)
	now := time.Now().UTC()
	demand := Demand{ClusterID: "cluster-a", SandboxID: "sandbox-1", VCPU: 1, MemoryMiB: 4096}

	require.NoError(t, store.Upsert(t.Context(), demand, now.Add(time.Minute)))
	require.NoError(t, store.Fulfill(t.Context(), demand.ClusterID, demand.SandboxID))
	require.NoError(t, store.Fulfill(t.Context(), demand.ClusterID, demand.SandboxID))

	summary, err := store.Summary(t.Context(), demand.ClusterID, now)
	require.NoError(t, err)
	require.Equal(t, Summary{TotalFulfilled: 1}, summary)
}

func TestRedisStoreRecordsDirectSuccessOnlyWhileDemandIsPending(t *testing.T) {
	t.Parallel()

	client := redisutils.SetupInstance(t)
	store := NewRedisStore(client)
	now := time.Now().UTC()

	require.NoError(t, store.RecordSuccess(t.Context(), "cluster-a"))
	summary, err := store.Summary(t.Context(), "cluster-a", now)
	require.NoError(t, err)
	require.Equal(t, Summary{}, summary)

	demand := Demand{ClusterID: "cluster-a", SandboxID: "waiting", VCPU: 1, MemoryMiB: 4096}
	require.NoError(t, store.Upsert(t.Context(), demand, now.Add(time.Minute)))
	require.NoError(t, store.RecordSuccess(t.Context(), demand.ClusterID))

	summary, err = store.Summary(t.Context(), demand.ClusterID, now)
	require.NoError(t, err)
	require.Equal(t, Summary{Count: 1, VCPU: 1, MemoryMiB: 4096, TotalDirectSuccess: 1}, summary)
}

func TestRedisStoreKeepsDemandWhenFulfilledCounterIsInvalid(t *testing.T) {
	t.Parallel()

	client := redisutils.SetupInstance(t)
	store := NewRedisStore(client)
	now := time.Now().UTC()
	demand := Demand{ClusterID: "cluster-a", SandboxID: "sandbox-1", VCPU: 1, MemoryMiB: 4096}

	require.NoError(t, store.Upsert(t.Context(), demand, now.Add(time.Minute)))
	entriesKey, deadlinesKey, fulfilledKey := redisKeys(demand.ClusterID)
	require.NoError(t, client.Set(t.Context(), fulfilledKey, "not-an-integer", 0).Err())

	require.Error(t, store.Fulfill(t.Context(), demand.ClusterID, demand.SandboxID))
	require.True(t, client.HExists(t.Context(), entriesKey, demand.SandboxID).Val())
	require.NoError(t, client.ZScore(t.Context(), deadlinesKey, demand.SandboxID).Err())
	_, err := store.Summary(t.Context(), demand.ClusterID, now)
	require.Error(t, err)
}

func TestRedisStoreRejectsInvalidDemand(t *testing.T) {
	t.Parallel()

	client := redisutils.SetupInstance(t)
	store := NewRedisStore(client)
	now := time.Now().UTC()

	tests := []Demand{
		{SandboxID: "sandbox", VCPU: 1, MemoryMiB: 4096},
		{ClusterID: "cluster", VCPU: 1, MemoryMiB: 4096},
		{ClusterID: "cluster", SandboxID: "sandbox", MemoryMiB: 4096},
		{ClusterID: "cluster", SandboxID: "sandbox", VCPU: 1},
	}
	for _, demand := range tests {
		require.Error(t, store.Upsert(t.Context(), demand, now.Add(time.Minute)))
	}
	require.Error(t, store.Upsert(t.Context(), Demand{
		ClusterID: "cluster",
		SandboxID: "sandbox",
		VCPU:      1,
		MemoryMiB: 4096,
	}, now))
	require.Error(t, store.Fulfill(t.Context(), "", "sandbox"))
	require.Error(t, store.Remove(t.Context(), "cluster", ""))
	_, err := store.Summary(t.Context(), "", now)
	require.Error(t, err)
}

func TestRedisStoreRejectsMissingClient(t *testing.T) {
	t.Parallel()

	var store *RedisStore
	now := time.Now().UTC()
	demand := Demand{ClusterID: "cluster", SandboxID: "sandbox", VCPU: 1, MemoryMiB: 4096}

	require.Error(t, store.Upsert(t.Context(), demand, now.Add(time.Minute)))
	require.Error(t, store.Fulfill(t.Context(), demand.ClusterID, demand.SandboxID))
	require.Error(t, store.Remove(t.Context(), demand.ClusterID, demand.SandboxID))
	_, err := store.Summary(t.Context(), demand.ClusterID, now)
	require.Error(t, err)
}
