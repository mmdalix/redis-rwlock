-- release.lua — free a holder, then immediately grant the next eligible waiter(s).
--
-- KEYS[1] = prefix
-- ARGV    = token, notify_key_ttl_ms
-- Returns: { "OK" } | { "NOT_HELD" }

local prefix = KEYS[1]
local k = keys_for(prefix)
local token = ARGV[1]
local notify_ttl = tonumber(ARGV[2])

local now = now_ms()
sweep(k, now)

local freed = false
if redis.call('EXISTS', k.writer) == 1 and redis.call('HGET', k.writer, 'token') == token then
  redis.call('DEL', k.writer)
  freed = true
elseif redis.call('ZSCORE', k.readers, token) then
  redis.call('ZREM', k.readers, token)
  freed = true
end

if not freed then
  return { 'NOT_HELD' }
end

grant_from_queue(k, prefix, now, notify_ttl)
return { 'OK' }
