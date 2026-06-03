-- GENERATED from protocol/lua/cancel_wait.lua — DO NOT EDIT (run tools/sync-lua).
-- cancel_wait.lua — remove a timed-out/cancelled waiter; reconcile a last-instant grant.
--
-- KEYS[1] = prefix
-- ARGV    = request_id, notify_key_ttl_ms
-- Returns:
--   { "CANCELLED" }          still queued; removed; grant_from_queue re-run if it was head
--   { "RECLAIMED", token }   already granted at the buzzer; that holder released and handed on
--   { "GONE" }               request not found (already cleaned)

local prefix = KEYS[1]
local k = keys_for(prefix)
local request_id = ARGV[1]
local notify_ttl = tonumber(ARGV[2])

local now = now_ms()
local rk = req_key(prefix, request_id)
if redis.call('EXISTS', rk) == 0 then
  redis.call('ZREM', k.queue, request_id)   -- drop any orphan queue entry left behind
  return { 'GONE' }
end

local granted = redis.call('HGET', rk, 'granted_token')
if is_blank(granted) then
  local head = redis.call('ZRANGE', k.queue, 0, 0)
  local was_head = (#head > 0 and head[1] == request_id)
  redis.call('ZREM', k.queue, request_id)
  redis.call('DEL', rk)
  if was_head then grant_from_queue(k, prefix, now, notify_ttl) end
  return { 'CANCELLED' }
end

-- already granted at the last instant -> release that holder (writer or reader) and hand on
if redis.call('EXISTS', k.writer) == 1 and redis.call('HGET', k.writer, 'token') == granted then
  redis.call('DEL', k.writer)
elseif redis.call('ZSCORE', k.readers, granted) then
  redis.call('ZREM', k.readers, granted)
end
redis.call('DEL', rk)
grant_from_queue(k, prefix, now, notify_ttl)
return { 'RECLAIMED', granted }
