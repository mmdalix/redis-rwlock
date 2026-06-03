-- GENERATED from protocol/lua/acquire.lua — DO NOT EDIT (run tools/sync-lua).
-- acquire.lua — try an immediate grant; otherwise enqueue and return a mailbox.
--
-- KEYS[1] = prefix
-- ARGV    = mode, lease_ms, wait_ms, request_id, owner_id, fairness,
--           max_reader_batch, notify_key_ttl_ms, request_key_ttl_grace_ms
--
-- Returns:
--   { "GRANTED", token, fencing, lease_until_ms, mode }
--   { "QUEUED",  request_id, notify_key, wait_deadline_ms, next_wake_ms }
-- next_wake_ms is the soonest absolute time the waiter should self-wake (-1 if none).

local prefix     = KEYS[1]
local k          = keys_for(prefix)
local mode       = ARGV[1]
local lease_ms   = clamp(tonumber(ARGV[2]) or 0, 1, MAX_LEASE_MS)
local wait_ms    = clamp(tonumber(ARGV[3]) or 0, 0, MAX_WAIT_MS)
local request_id = ARGV[4]
local owner_id   = ARGV[5]
local fairness   = ARGV[6]
local max_rb     = clamp(tonumber(ARGV[7]) or 1000, 1, MAX_READER_BATCH)
local notify_ttl = tonumber(ARGV[8])
local req_grace  = tonumber(ARGV[9])

local now = now_ms()
sweep(k, now)
local queued_writers = prune_queue(k, prefix, now)
local has_writer = writer_held(k)
local readers = reader_count(k)
local queue_len = redis.call('ZCARD', k.queue)

local grantable = false
if mode == 'read' then
  -- read_preferring: proceed whenever no writer holds.
  -- write_preferring: proceed only if no writer is queued.
  -- fifo: proceed only if nothing is queued ahead.
  if fairness == 'read_preferring' then
    grantable = not has_writer
  elseif fairness == 'fifo' then
    grantable = (not has_writer) and queue_len == 0
  else
    grantable = (not has_writer) and queued_writers == 0
  end
elseif mode == 'write' then
  grantable = (not has_writer) and readers == 0 and queue_len == 0
end

if grantable then
  local fencing = redis.call('INCR', k.fence)
  local token = new_token(owner_id, request_id, fencing)
  local expire_at = now + lease_ms
  if mode == 'write' then
    redis.call('HSET', k.writer, 'token', token, 'expire_at_ms', expire_at)
    redis.call('PEXPIRE', k.writer, lease_ms)
  else
    redis.call('ZADD', k.readers, expire_at, token)
  end
  return { 'GRANTED', token, fencing, expire_at, mode }
end

-- enqueue
local seq = redis.call('INCR', k.seq)
local rk = req_key(prefix, request_id)
local wait_deadline = now + wait_ms
redis.call('HSET', rk,
  'mode', mode,
  'owner_id', owner_id,
  'lease_ms', lease_ms,
  'wait_deadline_ms', wait_deadline,
  'notify_key', notify_key(prefix, request_id),
  'granted_token', '',
  'created_at_ms', now,
  'fairness', fairness,
  'max_reader_batch', max_rb)
redis.call('PEXPIRE', rk, wait_ms + req_grace)
redis.call('ZADD', k.queue, seq, request_id)

return { 'QUEUED', request_id, notify_key(prefix, request_id), wait_deadline, next_wake(k, prefix, now) }
