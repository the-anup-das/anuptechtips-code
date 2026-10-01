-- ARGV: emission interval T (sec per request), burst B
local T = tonumber(ARGV[1])
local tau = (tonumber(ARGV[2]) - 1) * T        -- how far ahead a client may run
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

local tat = math.max(tonumber(redis.call('GET', KEYS[1])) or now, now)
if tat - now > tau then
  return {0, math.ceil((tat - tau - now) * 1000)}   -- denied: wait this many ms
end
redis.call('SET', KEYS[1], tat + T, 'PX', math.ceil((tat + T - now) * 1000))
return {1, 0}
