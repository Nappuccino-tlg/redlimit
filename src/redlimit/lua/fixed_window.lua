-- Fixed window: spend `cost` from every key at once, or from none of them.
--
-- KEYS  one or more limiter keys, already namespaced by the caller
-- ARGV  limit, window seconds, cost
-- ->    {allowed, remaining, retry_after_ms}
--
-- Two decisions worth knowing about.
--
-- The window boundary comes from the Redis server clock rather than the caller's. Several
-- application processes on slightly skewed clocks would otherwise disagree about which
-- window they are in, and between them let through more than the limit at every boundary.
-- One clock, one answer.
--
-- A denied attempt is rolled back. Incrementing and leaving it there would mean a caller
-- who keeps trying holds their own counter above the limit forever, so the window would
-- never let them back in -- and the limit would quietly mean "per window, plus however
-- many rejections you collected". Rolling back makes it mean exactly what it says: this
-- many successful spends per window.

local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])

local slot = math.floor(tonumber(redis.call('TIME')[1]) / window)

local keys = {}
for i, key in ipairs(KEYS) do
  keys[i] = key .. ':' .. slot
end

-- Worst key decides for all of them: an attempt is allowed only if every key it touches
-- can afford it.
local worst, worst_at = -1, 1
for i, key in ipairs(keys) do
  local count = redis.call('INCRBY', key, cost)
  -- Not "if count == cost": a key that somehow lost its TTL would never regain one and
  -- would hold its count for good.
  if redis.call('TTL', key) < 0 then
    redis.call('EXPIRE', key, window)
  end
  if count > worst then
    worst, worst_at = count, i
  end
end

if worst > limit then
  for _, key in ipairs(keys) do
    redis.call('DECRBY', key, cost)
  end
  return {0, 0, redis.call('PTTL', keys[worst_at])}
end

return {1, limit - worst, redis.call('PTTL', keys[worst_at])}
