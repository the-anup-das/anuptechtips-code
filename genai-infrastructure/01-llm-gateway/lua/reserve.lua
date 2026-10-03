-- Reserve `amount` at every level of the hierarchy, or at none of them.
-- KEYS[1]  = the reservation
-- KEYS[2..] = one (spent, limit) pair per level: org, team, tenant, key
-- ARGV[1]  = amount in micro-USD: the priced worst case, input + max_tokens
-- ARGV[2]  = reservation TTL in seconds
if redis.call('EXISTS', KEYS[1]) == 1 then
  return {2, 0}                               -- this ID already holds a reservation: a retry
end
local amount = tonumber(ARGV[1])
for i = 2, #KEYS, 2 do
  local spent = tonumber(redis.call('GET', KEYS[i]) or '0')
  local limit = tonumber(redis.call('GET', KEYS[i + 1]) or '')
  if limit and spent + amount > limit then
    return {0, i / 2}                         -- level i/2 said no, and nothing was debited
  end
end
for i = 2, #KEYS, 2 do
  redis.call('INCRBY', KEYS[i], amount)
  redis.call('EXPIRE', KEYS[i], 3456000, 'NX')  -- a month's counter is kept for 40 days
end
redis.call('SET', KEYS[1], amount, 'EX', ARGV[2])
return {1, 0}
