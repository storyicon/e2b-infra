package workload

import "github.com/redis/go-redis/v9"

var (
	acquireScript = redis.NewScript(`
		local server_time = redis.call('TIME')
		local now_ms = (tonumber(server_time[1]) * 1000) + math.floor(tonumber(server_time[2]) / 1000)
		local expires_at_ms = tonumber(ARGV[3])
		if not expires_at_ms or expires_at_ms <= now_ms then
			return redis.error_reply('workload expiry must be in the future')
		end
		local payload = redis.call('HGET', KEYS[1], ARGV[1])
		if payload then
			local deadline = redis.call('ZSCORE', KEYS[2], ARGV[1])
			if not deadline then
				return redis.error_reply('workload entry is missing its deadline')
			end
			local ok, record = pcall(cjson.decode, payload)
			if not ok or type(record) ~= 'table' or record.schema_version ~= 2 or
				type(record.execution_id) ~= 'string' or
				(record.state ~= 'starting' and record.state ~= 'running') or
				type(record.expires_at_ms) ~= 'number' then
				return redis.error_reply('workload payload is invalid')
			end
			local deadline_number = tonumber(deadline)
			if not deadline_number or deadline_number ~= record.expires_at_ms then
				return redis.error_reply('workload deadline is invalid')
			end
			if deadline_number > now_ms then
				if record.execution_id == ARGV[2] then
					return 0
				end
				return -1
			end
			redis.call('HDEL', KEYS[1], ARGV[1])
		end
		redis.call('ZREM', KEYS[2], ARGV[1])
		local record = {
			schema_version = 2,
			execution_id = ARGV[2],
			state = 'starting',
			expires_at_ms = expires_at_ms
		}
		redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(record))
		redis.call('ZADD', KEYS[2], expires_at_ms, ARGV[1])
		return 1
	`)
	transitionScript = redis.NewScript(`
		local server_time = redis.call('TIME')
		local now_ms = (tonumber(server_time[1]) * 1000) + math.floor(tonumber(server_time[2]) / 1000)
		local expires_at_ms = tonumber(ARGV[3])
		if not expires_at_ms or expires_at_ms <= now_ms then
			return redis.error_reply('workload expiry must be in the future')
		end
		local payload = redis.call('HGET', KEYS[1], ARGV[1])
		if not payload then
			redis.call('ZREM', KEYS[2], ARGV[1])
			return 0
		end
		local deadline = redis.call('ZSCORE', KEYS[2], ARGV[1])
		if not deadline then
			return redis.error_reply('workload entry is missing its deadline')
		end
		local ok, record = pcall(cjson.decode, payload)
		if not ok or type(record) ~= 'table' or record.schema_version ~= 2 or
			type(record.execution_id) ~= 'string' or
			(record.state ~= 'starting' and record.state ~= 'running') or
			type(record.expires_at_ms) ~= 'number' then
			return redis.error_reply('workload payload is invalid')
		end
		local deadline_number = tonumber(deadline)
		if not deadline_number or deadline_number ~= record.expires_at_ms then
			return redis.error_reply('workload deadline is invalid')
		end
		if record.execution_id ~= ARGV[2] then
			return 0
		end
		if deadline_number <= now_ms then
			redis.call('HDEL', KEYS[1], ARGV[1])
			redis.call('ZREM', KEYS[2], ARGV[1])
			return 0
		end
		if record.state ~= ARGV[4] and record.state ~= ARGV[5] then
			return -1
		end
		record.state = ARGV[5]
		record.expires_at_ms = expires_at_ms
		redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(record))
		redis.call('ZADD', KEYS[2], expires_at_ms, ARGV[1])
		return 1
	`)
	removeScript = redis.NewScript(`
		local payload = redis.call('HGET', KEYS[1], ARGV[1])
		if not payload then
			redis.call('ZREM', KEYS[2], ARGV[1])
			return 0
		end
		local ok, record = pcall(cjson.decode, payload)
		if not ok or type(record) ~= 'table' or record.schema_version ~= 2 or
			type(record.execution_id) ~= 'string' then
			return redis.error_reply('workload payload is invalid')
		end
		if record.execution_id ~= ARGV[2] then
			return 0
		end
		redis.call('HDEL', KEYS[1], ARGV[1])
		redis.call('ZREM', KEYS[2], ARGV[1])
		return 1
	`)
	countScript = redis.NewScript(`
		local server_time = redis.call('TIME')
		local now_ms = (tonumber(server_time[1]) * 1000) + math.floor(tonumber(server_time[2]) / 1000)
		return redis.call('ZCOUNT', KEYS[1], '(' .. now_ms, '+inf')
	`)
	sweepScript = redis.NewScript(`
		local server_time = redis.call('TIME')
		local now_ms = (tonumber(server_time[1]) * 1000) + math.floor(tonumber(server_time[2]) / 1000)
		local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now_ms, 'LIMIT', 0, ARGV[1])
		if #expired == 0 then
			return 0
		end
		redis.call('HDEL', KEYS[1], unpack(expired))
		redis.call('ZREM', KEYS[2], unpack(expired))
		return #expired
	`)
)
