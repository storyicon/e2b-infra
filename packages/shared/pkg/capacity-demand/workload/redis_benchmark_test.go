package workload

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func BenchmarkRedisStoreAcquire(b *testing.B) {
	client := setupBenchmarkRedis(b)
	store := NewRedisStore(client)
	now := time.Now().UTC()
	var sequence atomic.Uint64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := sequence.Add(1)
			if _, err := store.Acquire(b.Context(), "benchmark", fmt.Sprintf("sandbox-%d", id), fmt.Sprintf("execution-%d", id), now, now.Add(time.Hour)); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRedisStoreRenew(b *testing.B) {
	client := setupBenchmarkRedis(b)
	store := NewRedisStore(client)
	now := time.Now().UTC()
	const records = 1024
	for index := range records {
		id := fmt.Sprintf("sandbox-%d", index)
		if _, err := store.Acquire(b.Context(), "benchmark", id, "execution", now, now.Add(time.Hour)); err != nil {
			b.Fatal(err)
		}
	}
	var sequence atomic.Uint64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := fmt.Sprintf("sandbox-%d", sequence.Add(1)%records)
			if _, err := store.Renew(b.Context(), "benchmark", id, "execution", now, now.Add(time.Hour)); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRedisStoreCount10000(b *testing.B) {
	client := setupBenchmarkRedis(b)
	store := NewRedisStore(client)
	expiresAt := time.Now().Add(time.Hour).UnixMilli()
	pipe := client.Pipeline()
	for index := range 10_000 {
		id := fmt.Sprintf("sandbox-%d", index)
		pipe.HSet(b.Context(), entriesKey("benchmark"), id, `{}`)
		pipe.ZAdd(b.Context(), deadlinesKey("benchmark"), redis.Z{Member: id, Score: float64(expiresAt)})
	}
	if _, err := pipe.Exec(b.Context()); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for b.Loop() {
		if _, err := store.Count(b.Context(), "benchmark"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedisStoreSweepExpired10000(b *testing.B) {
	client := setupBenchmarkRedis(b)
	store := NewRedisStore(client)
	for b.Loop() {
		b.StopTimer()
		if err := client.FlushDB(b.Context()).Err(); err != nil {
			b.Fatal(err)
		}
		pipe := client.Pipeline()
		expiresAt := time.Now().Add(-time.Minute).UnixMilli()
		for index := range 10_000 {
			id := fmt.Sprintf("sandbox-%d", index)
			pipe.HSet(b.Context(), entriesKey("benchmark"), id, `{}`)
			pipe.ZAdd(b.Context(), deadlinesKey("benchmark"), redis.Z{Member: id, Score: float64(expiresAt)})
		}
		if _, err := pipe.Exec(b.Context()); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		for {
			removed, err := store.SweepExpired(b.Context(), "benchmark", maxSweepBatch)
			if err != nil {
				b.Fatal(err)
			}
			if removed == 0 {
				break
			}
		}
	}
}

func setupBenchmarkRedis(b *testing.B) redis.UniversalClient {
	b.Helper()
	address := os.Getenv("WORKLOAD_TEST_REDIS_ADDR")
	if address == "" {
		b.Skip("WORKLOAD_TEST_REDIS_ADDR is required")
	}

	client := redis.NewClient(&redis.Options{Addr: address})
	if err := client.Ping(b.Context()).Err(); err != nil {
		b.Fatal(err)
	}
	if err := client.FlushDB(b.Context()).Err(); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = client.FlushDB(context.WithoutCancel(b.Context())).Err()
		_ = client.Close()
	})

	return client
}
