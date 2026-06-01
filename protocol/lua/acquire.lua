-- acquire.lua — try an immediate grant; otherwise enqueue and return a mailbox.
--
-- KEYS[1] = prefix "rwlock:{<resource>}"
-- ARGV    = mode, lease_ms, wait_ms, request_id, owner_id, fairness,
--           max_reader_batch, notify_key_ttl_ms, request_key_ttl_grace_ms
--
-- Returns:
--   { "GRANTED", token, fencing, lease_until_ms, mode }
--   { "QUEUED",  request_id, notify_key, wait_deadline_ms, head_holder_lease_until_ms }

local prefix          = KEYS[1]
local k               = keys_for(prefix)
local mode            = ARGV[1]
local lease_ms        = tonumber(ARGV[2])
local wait_ms         = tonumber(ARGV[3])
local request_id      = ARGV[4]
local owner_id        = ARGV[5]
local fairness        = ARGV[6]
local max_reader_batch= tonumber(ARGV[7])
local notify_ttl      = tonumber(ARGV[8])
local req_ttl_grace   = tonumber(ARGV[9])

local now = now_ms()
sweep(k, now)
drop_timed_out_head_requests(k, prefix, now)

local writer_token   = redis.call('HGET', k.state, 'writer_token')
local reader_count   = tonumber(redis.call('HGET', k.state, 'reader_count')) or 0
local queued_writers = tonumber(redis.call('HGET', k.state, 'queued_writers')) or 0
local queue_len      = redis.call('ZCARD', k.queue)
local has_writer     = not is_blank(writer_token)

local grantable = false
if mode == 'read' then
  grantable = (not has_writer) and (fairness == 'read_preferring' or queued_writers == 0)
elseif mode == 'write' then
  -- a writer takes a free lock only when nothing is queued ahead of it
  grantable = (not has_writer) and reader_count == 0 and queue_len == 0
end

if grantable then
  local fencing = redis.call('INCR', k.fence)
  local token = owner_id .. ':' .. request_id .. ':' .. fencing
  local expire_at = now + lease_ms
  redis.call('ZADD', k.holders, expire_at, token)
  redis.call('HSET', k.holder_meta, token, cjson.encode({
    mode = mode, owner_id = owner_id, fencing = fencing, request_id = request_id }))
  if mode == 'write' then
    redis.call('HSET', k.state, 'mode', 'write', 'writer_token', token, 'reader_count', 0)
  else
    redis.call('HSET', k.state, 'mode', 'read', 'writer_token', '', 'reader_count', reader_count + 1)
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
  'max_reader_batch', max_reader_batch)
redis.call('PEXPIRE', rk, wait_ms + req_ttl_grace)
redis.call('ZADD', k.queue, seq, request_id)
if mode == 'write' then
  redis.call('HSET', k.state, 'queued_writers', queued_writers + 1)
end

-- soonest current-holder expiry, so the client can size its self-wake (-1 if none)
local hh = redis.call('ZRANGE', k.holders, 0, 0, 'WITHSCORES')
local head_holder_lease = -1
if #hh >= 2 then head_holder_lease = tonumber(hh[2]) end

return { 'QUEUED', request_id, notify_key(prefix, request_id), wait_deadline, head_holder_lease }
