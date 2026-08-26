package capacitydemand

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	keyPrefix       = "e2b:capacity-demand"
	maxClusterIDLen = 128
	maxSandboxIDLen = 256
)

var (
	upsertScript = redis.NewScript(`
		redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
		redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
		return 1
	`)
	fulfillScript = redis.NewScript(`
		if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 0 then
			redis.call('ZREM', KEYS[2], ARGV[1])
			return 0
		end
		redis.call('INCR', KEYS[3])
		redis.call('HDEL', KEYS[1], ARGV[1])
		redis.call('ZREM', KEYS[2], ARGV[1])
		return 1
	`)
	recordSuccessScript = redis.NewScript(`
		if redis.call('HLEN', KEYS[1]) == 0 then
			return 0
		end
		redis.call('INCR', KEYS[2])
		return 1
	`)
	removeScript = redis.NewScript(`
		redis.call('HDEL', KEYS[1], ARGV[1])
		redis.call('ZREM', KEYS[2], ARGV[1])
		return 1
	`)
	activeDemandsScript = redis.NewScript(`
		local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1])
		if #expired > 0 then
			redis.call('HDEL', KEYS[1], unpack(expired))
			redis.call('ZREM', KEYS[2], unpack(expired))
		end
		local values = redis.call('HVALS', KEYS[1])
		table.insert(values, 1, redis.call('GET', KEYS[4]) or '0')
		table.insert(values, 1, redis.call('GET', KEYS[3]) or '0')
		return values
	`)
)

type Demand struct {
	ClusterID string `json:"cluster_id"`
	SandboxID string `json:"sandbox_id"`
	VCPU      int64  `json:"vcpu"`
	MemoryMiB int64  `json:"memory_mib"`
}

type Summary struct {
	Count              int64
	VCPU               int64
	MemoryMiB          int64
	TotalFulfilled     int64
	TotalDirectSuccess int64
}

type RedisStore struct {
	client redis.UniversalClient
}

func NewRedisStore(client redis.UniversalClient) *RedisStore {
	return &RedisStore{client: client}
}

func (s *RedisStore) Upsert(ctx context.Context, demand Demand, expiresAt time.Time) error {
	if err := validateDemand(demand); err != nil {
		return err
	}
	if s == nil || s.client == nil {
		return errors.New("capacity demand Redis client is required")
	}
	if !expiresAt.After(time.Now()) {
		return errors.New("capacity demand expiry must be in the future")
	}

	payload, err := json.Marshal(demand)
	if err != nil {
		return fmt.Errorf("encode capacity demand: %w", err)
	}
	entriesKey, deadlinesKey, _ := redisKeys(demand.ClusterID)
	if err := upsertScript.Run(ctx, s.client, []string{entriesKey, deadlinesKey}, demand.SandboxID, payload, expiresAt.UnixMilli()).Err(); err != nil {
		return fmt.Errorf("upsert capacity demand: %w", err)
	}

	return nil
}

func (s *RedisStore) Fulfill(ctx context.Context, clusterID, sandboxID string) error {
	if err := validateIDs(clusterID, sandboxID); err != nil {
		return err
	}
	if s == nil || s.client == nil {
		return errors.New("capacity demand Redis client is required")
	}

	entriesKey, deadlinesKey, fulfilledKey := redisKeys(clusterID)
	if err := fulfillScript.Run(ctx, s.client, []string{entriesKey, deadlinesKey, fulfilledKey}, sandboxID).Err(); err != nil {
		return fmt.Errorf("fulfill capacity demand: %w", err)
	}

	return nil
}

// RecordSuccess counts capacity consumed by an owner request that succeeded
// without first entering the pending ledger. It only advances the monotonic
// counter while another demand in the cluster is pending, which makes the
// success part of the active burst rather than a future one.
func (s *RedisStore) RecordSuccess(ctx context.Context, clusterID string) error {
	if err := validateClusterID(clusterID); err != nil {
		return err
	}
	if s == nil || s.client == nil {
		return errors.New("capacity demand Redis client is required")
	}

	entriesKey, _, _ := redisKeys(clusterID)
	if err := recordSuccessScript.Run(ctx, s.client, []string{entriesKey, directSuccessKey(clusterID)}).Err(); err != nil {
		return fmt.Errorf("record capacity success: %w", err)
	}

	return nil
}

func (s *RedisStore) Remove(ctx context.Context, clusterID, sandboxID string) error {
	if err := validateIDs(clusterID, sandboxID); err != nil {
		return err
	}
	if s == nil || s.client == nil {
		return errors.New("capacity demand Redis client is required")
	}

	entriesKey, deadlinesKey, _ := redisKeys(clusterID)
	if err := removeScript.Run(ctx, s.client, []string{entriesKey, deadlinesKey}, sandboxID).Err(); err != nil {
		return fmt.Errorf("remove capacity demand: %w", err)
	}

	return nil
}

func (s *RedisStore) Summary(ctx context.Context, clusterID string, now time.Time) (Summary, error) {
	if err := validateClusterID(clusterID); err != nil {
		return Summary{}, err
	}
	if s == nil || s.client == nil {
		return Summary{}, errors.New("capacity demand Redis client is required")
	}

	entriesKey, deadlinesKey, fulfilledKey := redisKeys(clusterID)
	values, err := activeDemandsScript.Run(ctx, s.client, []string{entriesKey, deadlinesKey, fulfilledKey, directSuccessKey(clusterID)}, now.UnixMilli()).StringSlice()
	if err != nil {
		return Summary{}, fmt.Errorf("read capacity demands: %w", err)
	}

	if len(values) < 2 {
		return Summary{}, errors.New("read capacity demands: missing success counters")
	}

	totalFulfilled, err := strconv.ParseInt(values[0], 10, 64)
	if err != nil || totalFulfilled < 0 {
		return Summary{}, fmt.Errorf("decode capacity demand total fulfilled %q", values[0])
	}
	totalDirectSuccess, err := strconv.ParseInt(values[1], 10, 64)
	if err != nil || totalDirectSuccess < 0 {
		return Summary{}, fmt.Errorf("decode capacity demand total direct success %q", values[1])
	}
	summary := Summary{TotalFulfilled: totalFulfilled, TotalDirectSuccess: totalDirectSuccess}
	for _, value := range values[2:] {
		var demand Demand
		if err := json.Unmarshal([]byte(value), &demand); err != nil {
			return Summary{}, fmt.Errorf("decode capacity demand: %w", err)
		}
		if err := validateDemand(demand); err != nil {
			return Summary{}, fmt.Errorf("invalid stored capacity demand: %w", err)
		}
		if demand.ClusterID != clusterID {
			return Summary{}, fmt.Errorf("stored capacity demand belongs to cluster %q, expected %q", demand.ClusterID, clusterID)
		}

		summary.Count++
		summary.VCPU += demand.VCPU
		summary.MemoryMiB += demand.MemoryMiB
	}

	return summary, nil
}

func redisKeys(clusterID string) (string, string, string) {
	hashTag := "{" + clusterID + "}"

	return keyPrefix + ":" + hashTag + ":entries", keyPrefix + ":" + hashTag + ":deadlines", keyPrefix + ":" + hashTag + ":total-fulfilled"
}

func directSuccessKey(clusterID string) string {
	return keyPrefix + ":{" + clusterID + "}:total-direct-success"
}

func validateDemand(demand Demand) error {
	if err := validateIDs(demand.ClusterID, demand.SandboxID); err != nil {
		return err
	}
	if demand.VCPU <= 0 {
		return errors.New("capacity demand vCPU must be positive")
	}
	if demand.MemoryMiB <= 0 {
		return errors.New("capacity demand memory must be positive")
	}

	return nil
}

func validateIDs(clusterID, sandboxID string) error {
	if err := validateClusterID(clusterID); err != nil {
		return err
	}
	if sandboxID == "" || len(sandboxID) > maxSandboxIDLen {
		return errors.New("capacity demand sandbox ID is required and must be at most 256 bytes")
	}

	return nil
}

func validateClusterID(clusterID string) error {
	if clusterID == "" || len(clusterID) > maxClusterIDLen || strings.ContainsAny(clusterID, "{}") {
		return errors.New("capacity demand cluster ID is required, must be at most 128 bytes, and cannot contain braces")
	}

	return nil
}
