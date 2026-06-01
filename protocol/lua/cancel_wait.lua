-- cancel_wait.lua — remove a timed-out/cancelled waiter; reconcile a last-instant grant.
--
-- KEYS[1] = prefix
-- ARGV    = request_id, notify_key_ttl_ms
--
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
  return { 'GONE' }
end

local granted = redis.call('HGET', rk, 'granted_token')
if is_blank(granted) then
  local head = redis.call('ZRANGE', k.queue, 0, 0)
  local was_head = (#head > 0 and head[1] == request_id)
  redis.call('ZREM', k.queue, request_id)
  if redis.call('HGET', rk, 'mode') == 'write' then dec_queued_writers(k) end
  redis.call('DEL', rk)
  if was_head then grant_from_queue(k, prefix, now, notify_ttl) end
  return { 'CANCELLED' }
end

-- already granted at the last instant -> release that holder and hand on
redis.call('ZREM', k.holders, granted)
redis.call('HDEL', k.holder_meta, granted)
redis.call('DEL', rk)
recompute_state_cache(k)
grant_from_queue(k, prefix, now, notify_ttl)
return { 'RECLAIMED', granted }
