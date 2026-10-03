-- Swap a reservation for what the call really cost.
-- KEYS[1]  = the reservation
-- KEYS[2..] = the same spent counters, one per level
-- ARGV[1]  = actual cost in micro-USD
local reserved = redis.call('GET', KEYS[1])
if not reserved then
  return -1          -- settled already, or expired: the full amount stays charged
end
local refund = tonumber(reserved) - tonumber(ARGV[1])
for i = 2, #KEYS do
  redis.call('DECRBY', KEYS[i], refund)
end
redis.call('DEL', KEYS[1])
return refund
