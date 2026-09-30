-- ARGV: limit, window (sec). One hash: w (window number), curr, prev.
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local w = math.floor(now / window)

local s = redis.call('HMGET', KEYS[1], 'w', 'curr', 'prev')
local sw, curr, prev = tonumber(s[1]) or w, tonumber(s[2]) or 0, tonumber(s[3]) or 0
if w == sw + 1 then
  prev, curr = curr, 0            -- the current window became the previous one
elseif w > sw + 1 then
  prev, curr = 0, 0               -- idle for more than a window
end

local elapsed = now - w * window
local estimate = prev * (1 - elapsed / window) + curr
if estimate + 1 <= limit then
  redis.call('HSET', KEYS[1], 'w', w, 'curr', curr + 1, 'prev', prev)
  redis.call('EXPIRE', KEYS[1], 2 * window)
  return {1, 0}
end

-- denied: how long until the estimate has room for one more?
local wait
if curr + 1 <= limit then
  wait = window * (1 - (limit - 1 - curr) / prev) - elapsed
else
  wait = (window - elapsed) + window * (1 - (limit - 1) / curr)
end
return {0, math.max(1, math.ceil(wait * 1000))}
