-- Leaky bucket as a queue. ARGV: drain rate (requests/sec), queue size.
-- Returns {1, delay_ms}: forward the request after delay_ms, or {0, 0}: full.
local rate = tonumber(ARGV[1])
local size = tonumber(ARGV[2])

local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

local state = redis.call('HMGET', KEYS[1], 'level', 'ts')
local level = tonumber(state[1]) or 0
local ts = tonumber(state[2]) or now
level = math.max(0, level - math.max(0, now - ts) * rate)   -- what drained

if level > size then
  return {0, 0}
end
redis.call('HSET', KEYS[1], 'level', level + 1, 'ts', now)
redis.call('EXPIRE', KEYS[1], math.ceil((level + 1) / rate) + 1)
return {1, math.ceil(level / rate * 1000)}
