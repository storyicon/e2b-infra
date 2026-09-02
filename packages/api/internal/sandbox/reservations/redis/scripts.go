package redis

import (
	"fmt"

	"github.com/redis/go-redis/v9"
)

const (
	// Reserve result codes
	reserveResultReserved         = 0
	reserveResultAlreadyInStorage = 1
	reserveResultAlreadyPending   = 2
	reserveResultLimitExceeded    = 3
)

var (
	// reserveScript is the legacy protocol. Keeping its Redis shape unchanged
	// preserves the disabled-by-default path during a rolling deployment.
	reserveScript = redis.NewScript(fmt.Sprintf(`
		while true do
			local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[4], 'LIMIT', 0, 1000)
			if #expired == 0 then break end
			redis.call('ZREM', KEYS[2], unpack(expired))
			redis.call('HDEL', KEYS[4], unpack(expired))
		end
		if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 1 then return %d end
		if redis.call('ZSCORE', KEYS[2], ARGV[1]) then return %d end
		local limit = tonumber(ARGV[2])
		if limit >= 0 then
			local storageCount = redis.call('SCARD', KEYS[1])
			local pendingCount = redis.call('ZCARD', KEYS[2])
			if storageCount + pendingCount >= limit then return %d end
		end
		redis.call('DEL', KEYS[3])
		redis.call('HDEL', KEYS[4], ARGV[1])
		redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
		return %d
	`, reserveResultAlreadyInStorage, reserveResultAlreadyPending, reserveResultLimitExceeded, reserveResultReserved))

	finishStartScript = redis.NewScript(`
		redis.call('ZREM', KEYS[1], ARGV[1])
		redis.call('SET', KEYS[2], ARGV[2], 'EX', tonumber(ARGV[3]))
		return 1
	`)

	// A general sandbox removal may release only a legacy reservation. Owned
	// reservations are completed by their fenced owner callback.
	releaseScript = redis.NewScript(`
		if redis.call('HEXISTS', KEYS[3], ARGV[1]) == 1 then return 0 end
		redis.call('ZREM', KEYS[1], ARGV[1])
		redis.call('DEL', KEYS[2])
		return 1
	`)

	// reserveOwnedScript atomically checks limits and reserves a sandbox for creation.
	// The pending set is a ZSET where score = Unix timestamp of reservation.
	// Stale entries (older than staleTTL) are cleaned up before counting.
	//
	// KEYS[1] = storage index key (sandbox:storage:{teamID}:index)
	// KEYS[2] = pending zset key (sandbox:storage:{teamID}:reservations:pending)
	// KEYS[3] = result key (sandbox:storage:{teamID}:reservations:sandboxID:result)
	// KEYS[4] = owner token hash
	// ARGV[1] = sandboxID
	// ARGV[2] = limit (-1 means no limit)
	// ARGV[3] = current Unix timestamp (seconds, float)
	// ARGV[4] = stale cutoff Unix timestamp (now - staleTTL)
	// ARGV[5] = owner token
	//
	// Returns:
	//   0 = RESERVED (sandbox added to pending zset)
	//   1 = ALREADY_IN_STORAGE (sandbox exists in storage index)
	//   2 = ALREADY_PENDING (sandbox already in pending zset)
	//   3 = LIMIT_EXCEEDED (total count >= limit)
	reserveOwnedScript = redis.NewScript(fmt.Sprintf(`
		-- Clean up stale pending entries and their fencing tokens in bounded batches.
		while true do
			local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[4], 'LIMIT', 0, 1000)
			if #expired == 0 then
				break
			end
			redis.call('ZREM', KEYS[2], unpack(expired))
			redis.call('HDEL', KEYS[4], unpack(expired))
		end

		-- Check if sandbox already exists in storage index
		if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 1 then
			return %d
		end

		-- Check if sandbox is already pending (has a score in the zset)
		if redis.call('ZSCORE', KEYS[2], ARGV[1]) then
			return %d
		end

		-- Check limit (ARGV[2] < 0 means no limit)
		local limit = tonumber(ARGV[2])
		if limit >= 0 then
			local storageCount = redis.call('SCARD', KEYS[1])
			local pendingCount = redis.call('ZCARD', KEYS[2])
			if storageCount + pendingCount >= limit then
				return %d
			end
		end

		-- Delete stale result key from a previous failed attempt
		redis.call('DEL', KEYS[3])
		-- Reserve: add to pending zset with current timestamp as score
		redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
		redis.call('HSET', KEYS[4], ARGV[1], ARGV[5])
		return %d
	`, reserveResultAlreadyInStorage, reserveResultAlreadyPending, reserveResultLimitExceeded, reserveResultReserved))

	// finishOwnedStartScript removes a sandbox from the pending zset and sets the result key.
	// KEYS[1] = pending zset key
	// KEYS[2] = result key
	// KEYS[3] = owner token hash
	// ARGV[1] = sandboxID
	// ARGV[2] = owner token
	// ARGV[3] = result JSON
	// ARGV[4] = TTL in seconds
	finishOwnedStartScript = redis.NewScript(`
		if redis.call('HGET', KEYS[3], ARGV[1]) ~= ARGV[2] then
			return 0
		end
		redis.call('ZREM', KEYS[1], ARGV[1])
		redis.call('HDEL', KEYS[3], ARGV[1])
		redis.call('SET', KEYS[2], ARGV[3], 'EX', tonumber(ARGV[4]))
		return 1
	`)

	// heartbeatOwnedScript refreshes an existing reservation lease without ever
	// recreating one that has already been finished, released, or expired.
	// KEYS[1] = pending zset key
	// KEYS[2] = owner token hash
	// ARGV[1] = sandboxID
	// ARGV[2] = owner token
	// ARGV[3] = current Unix timestamp (seconds, float)
	//
	// Returns 1 when the reservation still exists and was refreshed, otherwise 0.
	heartbeatOwnedScript = redis.NewScript(`
		if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then
			return 0
		end
		if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then
			redis.call('HDEL', KEYS[2], ARGV[1])
			return 0
		end
		redis.call('ZADD', KEYS[1], 'XX', ARGV[3], ARGV[1])
		return 1
	`)

	// releaseOwnedScript removes a sandbox from the pending zset and deletes the result key.
	// KEYS[1] = pending zset key
	// KEYS[2] = result key
	// KEYS[3] = owner token hash
	// ARGV[1] = sandboxID
	// ARGV[2] = owner token
	releaseOwnedScript = redis.NewScript(`
		if redis.call('HGET', KEYS[3], ARGV[1]) ~= ARGV[2] then
			return 0
		end
		redis.call('ZREM', KEYS[1], ARGV[1])
		redis.call('HDEL', KEYS[3], ARGV[1])
		redis.call('DEL', KEYS[2])
		return 1
	`)
)
