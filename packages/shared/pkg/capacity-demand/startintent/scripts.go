package startintent

import "github.com/redis/go-redis/v9"

var (
	upsertScript = redis.NewScript(`
		local existing = redis.call('HGET', KEYS[1], ARGV[1])
		if existing then
			local deadline = redis.call('ZSCORE', KEYS[2], ARGV[1])
			if not deadline then
				return redis.error_reply('start intent is missing its deadline')
			end
			local deadline_number = tonumber(deadline)
			if not deadline_number then
				return redis.error_reply('start intent deadline is invalid')
			end
			if deadline_number > tonumber(ARGV[4]) then
				return 0
			end
			redis.call('HDEL', KEYS[1], ARGV[1])
		end
		redis.call('ZREM', KEYS[2], ARGV[1])
		redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
		redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
		return 1
	`)
	heartbeatScript = redis.NewScript(`
		local payload = redis.call('HGET', KEYS[1], ARGV[1])
		if not payload then
			redis.call('ZREM', KEYS[2], ARGV[1])
			return 0
		end
		local deadline = redis.call('ZSCORE', KEYS[2], ARGV[1])
		if not deadline then
			return redis.error_reply('start intent is missing its deadline')
		end
		local ok, record = pcall(cjson.decode, payload)
		if not ok or type(record) ~= 'table' or record.schema_version ~= 1 or
			type(record.owner_token) ~= 'string' or
			(record.state ~= 'outstanding' and record.state ~= 'handoff') or
			type(record.expires_at_ms) ~= 'number' then
			return redis.error_reply('start intent payload is invalid')
		end
		if record.owner_token ~= ARGV[2] then
			return 0
		end
		if record.state ~= 'outstanding' then
			return 0
		end
		local deadline_number = tonumber(deadline)
		if not deadline_number then
			return redis.error_reply('start intent deadline is invalid')
		end
		if deadline_number <= tonumber(ARGV[3]) then
			redis.call('HDEL', KEYS[1], ARGV[1])
			redis.call('ZREM', KEYS[2], ARGV[1])
			return 0
		end
		record.expires_at_ms = tonumber(ARGV[4])
		redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(record))
		redis.call('ZADD', KEYS[2], ARGV[4], ARGV[1])
		return 1
	`)
	handoffScript = redis.NewScript(`
		local payload = redis.call('HGET', KEYS[1], ARGV[1])
		if not payload then
			redis.call('ZREM', KEYS[2], ARGV[1])
			return 0
		end
		local deadline = redis.call('ZSCORE', KEYS[2], ARGV[1])
		if not deadline then
			return redis.error_reply('start intent is missing its deadline')
		end
		local ok, record = pcall(cjson.decode, payload)
		if not ok or type(record) ~= 'table' or record.schema_version ~= 1 or
			type(record.owner_token) ~= 'string' or
			(record.state ~= 'outstanding' and record.state ~= 'handoff') or
			type(record.expires_at_ms) ~= 'number' then
			return redis.error_reply('start intent payload is invalid')
		end
		if record.owner_token ~= ARGV[2] then
			return 0
		end
		if record.state ~= 'outstanding' then
			return 0
		end
		local deadline_number = tonumber(deadline)
		if not deadline_number then
			return redis.error_reply('start intent deadline is invalid')
		end
		if deadline_number <= tonumber(ARGV[3]) then
			redis.call('HDEL', KEYS[1], ARGV[1])
			redis.call('ZREM', KEYS[2], ARGV[1])
			return 0
		end
		record.state = 'handoff'
		record.expires_at_ms = tonumber(ARGV[4])
		redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(record))
		redis.call('ZADD', KEYS[2], ARGV[4], ARGV[1])
		return 1
	`)
	removeScript = redis.NewScript(`
		local payload = redis.call('HGET', KEYS[1], ARGV[1])
		if not payload then
			redis.call('ZREM', KEYS[2], ARGV[1])
			return 0
		end
		local ok, record = pcall(cjson.decode, payload)
		if not ok or type(record) ~= 'table' or record.schema_version ~= 1 or
			type(record.owner_token) ~= 'string' or
			(record.state ~= 'outstanding' and record.state ~= 'handoff') then
			return redis.error_reply('start intent payload is invalid')
		end
		if record.owner_token ~= ARGV[2] then
			return 0
		end
		redis.call('HDEL', KEYS[1], ARGV[1])
		redis.call('ZREM', KEYS[2], ARGV[1])
		return 1
	`)
	activeScript = redis.NewScript(`
		while true do
			local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1], 'LIMIT', 0, 1000)
			if #expired == 0 then
				break
			end
			redis.call('HDEL', KEYS[1], unpack(expired))
			redis.call('ZREM', KEYS[2], unpack(expired))
		end

		local deadline_members = redis.call('ZRANGE', KEYS[2], 0, -1)
		for _, sandbox_id in ipairs(deadline_members) do
			if redis.call('HEXISTS', KEYS[1], sandbox_id) == 0 then
				redis.call('ZREM', KEYS[2], sandbox_id)
			end
		end

		local sandbox_ids = redis.call('HKEYS', KEYS[1])
		local result = {}
		for _, sandbox_id in ipairs(sandbox_ids) do
			local payload = redis.call('HGET', KEYS[1], sandbox_id)
			local deadline = redis.call('ZSCORE', KEYS[2], sandbox_id)
			if not deadline then
				return redis.error_reply('start intent is missing its deadline')
			end
			table.insert(result, sandbox_id)
			table.insert(result, payload)
			table.insert(result, deadline)
		end
		return result
	`)
)
