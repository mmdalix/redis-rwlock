-- release.lua — free a holder, then immediately grant the next eligible waiter(s).
--
-- KEYS[1] = prefix
-- ARGV    = token, notify_key_ttl_ms
--
-- Returns: { "OK" } | { "NOT_HELD" }

local prefix = KEYS[1]
local k = keys_for(prefix)
local token = ARGV[1]
local notify_ttl = tonumber(ARGV[2])

local now = now_ms()

local meta = redis.call('HGET', k.holder_meta, token)
local score = redis.call('ZSCORE', k.holders, token)
if is_blank(meta) or score == false or score == nil then
  return { 'NOT_HELD' }
end

redis.call('ZREM', k.holders, token)
redis.call('HDEL', k.holder_meta, token)
recompute_state_cache(k)
grant_from_queue(k, prefix, now, notify_ttl)
return { 'OK' }
