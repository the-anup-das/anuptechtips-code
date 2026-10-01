-- ARGV: limit, window (sec). Windows line up with the clock (12:00, 12:01...).
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

local count = redis.call('INCR', KEYS[1])
if count == 1 then
  local t = redis.call('TIME')
  local window_end = (math.floor(tonumber(t[1]) / window) + 1) * window
  redis.call('EXPIREAT', KEYS[1], window_end)   -- set in the same atomic step
end
if count <= limit then
  return {1, 0}
end
return {0, redis.call('PTTL', KEYS[1])}
