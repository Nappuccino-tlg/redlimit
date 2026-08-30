-- Hand back what an attempt spent.
--
-- KEYS  the same keys the attempt was spent on
-- ARGV  window seconds, cost
-- ->    how many keys were actually credited
--
-- Only where the window still exists. A bare DECRBY against a key that expired between
-- the spend and the refund creates it again, holding a negative number and carrying no
-- TTL at all -- and that key then sits there absorbing the next window's traffic, and the
-- one after it, until somebody notices a limiter that stopped limiting.
--
-- The floor at zero covers the other direction: refunding more than was spent, which a
-- caller can do by mistake and should not be able to turn into free quota.

local window = tonumber(ARGV[1])
local cost = tonumber(ARGV[2])

local slot = math.floor(tonumber(redis.call('TIME')[1]) / window)

local credited = 0
for _, key in ipairs(KEYS) do
  local in_window = key .. ':' .. slot
  if redis.call('EXISTS', in_window) == 1 then
    if redis.call('DECRBY', in_window, cost) < 0 then
      redis.call('SET', in_window, 0, 'KEEPTTL')
    end
    credited = credited + 1
  end
end

return credited
