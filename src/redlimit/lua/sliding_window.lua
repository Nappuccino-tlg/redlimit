-- Sliding window counter: this window's spend plus a weighted share of the one before it.
--
-- KEYS  one or more limiter keys, already namespaced by the caller
-- ARGV  limit, window seconds, cost
-- ->    {allowed, remaining, retry_after_ms}
--
-- A fixed window lets a caller spend its whole budget just before a boundary and the
-- whole of the next one just after, so twice the limit can pass in a moment straddling
-- the two. This counts what is still inside a window that slides with the clock.
--
-- It is an approximation, and deliberately. The exact answer needs every timestamp kept
-- in a sorted set per key, which grows with the traffic being limited -- the busier the
-- caller, the more it costs to say no to them. This holds two integers per key however
-- busy they get, and its error only shows when traffic inside the previous window was
-- unevenly spread, because it assumes that window was uniform. For deciding whether
-- someone may try a password again, that is a trade worth making.

local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])

local time = redis.call('TIME')
local now = tonumber(time[1]) + tonumber(time[2]) / 1000000
local slot = math.floor(now / window)
-- How much of the previous window is still inside the sliding one: 1 at a boundary,
-- falling to 0 as this window fills up.
local carry = 1 - (now % window) / window

local current = {}
local worst, worst_at = -1, 1

for i, key in ipairs(KEYS) do
  local this_window = key .. ':' .. slot
  local last_window = key .. ':' .. (slot - 1)

  local count = redis.call('INCRBY', this_window, cost)
  if redis.call('TTL', this_window) < 0 then
    -- Twice the window, because the next one still has to be able to read this one.
    redis.call('EXPIRE', this_window, window * 2)
  end

  local previous = tonumber(redis.call('GET', last_window) or '0')
  local estimate = count + previous * carry

  current[i] = this_window
  if estimate > worst then
    worst, worst_at = estimate, i
  end
end

if worst > limit then
  -- Rolled back for the same reason as the fixed window: a rejection must not cost the
  -- caller anything, or the limit stops meaning what it says.
  for _, key in ipairs(current) do
    redis.call('DECRBY', key, cost)
  end
  -- Approximate, and only ever used as a hint: the estimate decays continuously, so it
  -- may well allow another attempt before this much time has passed.
  local ms = math.floor(((worst - limit) / worst) * window * 1000)
  return {0, 0, ms}
end

-- Floor, so remaining never rounds up into a spend that will not be there.
return {1, math.floor(limit - worst), redis.call('PTTL', current[worst_at])}
