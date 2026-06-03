-- GENERATED from protocol/lua/lib.lua — DO NOT EDIT (run tools/sync-lua).
-- lib.lua — shared helpers for all redis-rwlock scripts (PROTOCOL_VERSION 1).
--
-- This file is prepended to each script for the EVALSHA path and wrapped into the
-- Redis FUNCTION library; either way it is the single source of truth.
--
-- DESIGN: state is DERIVED from source-of-truth structures, never cached.
--   * readers : ZSET  member=reader token, score=expire_at_ms
--   * writer  : HASH  { token, expire_at_ms }  (the single writer; PEXPIRE'd to the
--               lease so a crashed writer fires a native keyspace `expired` event)
--   * queue   : ZSET  member=request_id, score=seq  (FIFO; O(log N) cancel)
--   * req:{id}: HASH  one per waiting request (see SPEC §4.5)            (+ TTL)
--   * notify:{id}: LIST  the waiter's private BLPOP mailbox              (+ TTL)
--   * seq, fence : STRING counters
-- There is NO denormalized state cache and NO per-holder metadata: a token is
-- self-describing ("owner_id:request_id:fencing"), and mode/reader_count/writer
-- presence are computed O(1) from `readers`/`writer`, so they cannot drift.
--
-- Conventions:
--   * KEYS[1] is the per-resource prefix "rwlock:{<resource>}" (carries the cluster
--     hash tag); every key is derived from it, so a call touches one slot only.
--   * Time is always read from the Redis server via TIME (never passed by clients).

-- Protocol-enforced limits (the brain owns policy, not the client) — SPEC §19.
local MAX_LEASE_MS = 300000
local MAX_WAIT_MS = 60000
local MAX_READER_BATCH = 1000000

local function now_ms()
  local t = redis.call('TIME')                       -- { seconds, microseconds }
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local function keys_for(prefix)
  return {
    readers = prefix .. ':readers',
    writer  = prefix .. ':writer',
    queue   = prefix .. ':queue',
    seq     = prefix .. ':seq',
    fence   = prefix .. ':fence',
  }
end

local function req_key(prefix, id)    return prefix .. ':req:' .. id end
local function notify_key(prefix, id) return prefix .. ':notify:' .. id end

local function is_blank(v)
  return v == nil or v == false or v == ''
end

local function clamp(v, lo, hi)
  if v < lo then return lo end
  if v > hi then return hi end
  return v
end

-- Evict expired readers, and clear the writer if its lease has lapsed. After sweep,
-- ZCARD readers and EXISTS writer are exact. (The writer key is also PEXPIRE'd, so an
-- idle crashed writer self-deletes and fires a keyspace event; this is the backstop.)
local function sweep(k, now)
  local dead = redis.call('ZRANGEBYSCORE', k.readers, 0, now)
  if #dead > 0 then
    redis.call('ZREMRANGEBYSCORE', k.readers, 0, now)
  end
  if redis.call('EXISTS', k.writer) == 1 then
    local exp = tonumber(redis.call('HGET', k.writer, 'expire_at_ms'))
    if exp == nil or exp <= now then redis.call('DEL', k.writer) end
  end
end

-- True iff a live writer currently holds. Call after sweep.
local function writer_held(k)
  return redis.call('EXISTS', k.writer) == 1
end

-- Number of live readers. Call after sweep.
local function reader_count(k)
  return redis.call('ZCARD', k.readers)
end

-- Prune the queue of orphaned (req hash gone) and timed-out-but-ungranted requests
-- anywhere in the queue, and RETURN the number of live queued writers — derived from
-- the surviving req hashes, never an incremental counter (which would drift on a
-- crashed queued writer and starve readers under write_preferring).
local function prune_queue(k, prefix, now)
  local members = redis.call('ZRANGE', k.queue, 0, -1)
  local writers = 0
  for _, id in ipairs(members) do
    local rk = req_key(prefix, id)
    local wd = redis.call('HGET', rk, 'wait_deadline_ms')
    local gt = redis.call('HGET', rk, 'granted_token')
    if is_blank(wd) then
      redis.call('ZREM', k.queue, id)                          -- orphan: req hash gone
    elseif tonumber(wd) <= now and is_blank(gt) then
      redis.call('ZREM', k.queue, id)
      redis.call('DEL', rk)                                    -- timed out, not granted
    elseif redis.call('HGET', rk, 'mode') == 'write' then
      writers = writers + 1
    end
  end
  return writers
end

-- The soonest absolute ms time at which something interesting could happen for a
-- waiter: a reader/writer lease boundary, or the head queued request's deadline.
-- Returned to waiters as their self-wake target (-1 if nothing). Fixes the lost-wakeup
-- where a crashed queued writer at the head left a reader with no boundary.
local function next_wake(k, prefix, now)
  local best = -1
  local r = redis.call('ZRANGE', k.readers, 0, 0, 'WITHSCORES')
  if #r >= 2 then best = tonumber(r[2]) end
  if redis.call('EXISTS', k.writer) == 1 then
    local we = tonumber(redis.call('HGET', k.writer, 'expire_at_ms'))
    if we and (best == -1 or we < best) then best = we end
  end
  local h = redis.call('ZRANGE', k.queue, 0, 0)
  if #h > 0 then
    local wd = tonumber(redis.call('HGET', req_key(prefix, h[1]), 'wait_deadline_ms'))
    if wd and (best == -1 or wd < best) then best = wd end
  end
  return best
end

-- Push a grant into a waiter's mailbox. The mailbox TTL is at least the grant's lease,
-- so a granted-but-undrained mailbox never expires before the holder it created.
local function push_grant(prefix, req_id, token, fencing, expire_at, mode, notify_ttl, now)
  local payload = cjson.encode({
    status = 'GRANTED', token = token, fencing = fencing,
    lease_until_ms = expire_at, mode = mode,
  })
  local nk = notify_key(prefix, req_id)
  redis.call('LPUSH', nk, payload)
  local ttl = expire_at - now
  if ttl < notify_ttl then ttl = notify_ttl end
  redis.call('PEXPIRE', nk, ttl)
end

local function new_token(owner_id, req_id, fencing)
  return owner_id .. ':' .. req_id .. ':' .. fencing
end

-- Grant the writer at request `req_id` (its req hash exists). Sets the writer key
-- (PEXPIRE'd to the lease), records granted_token, pushes the grant, dequeues.
local function grant_writer(k, prefix, req_id, now, notify_ttl)
  local rk = req_key(prefix, req_id)
  local owner_id = redis.call('HGET', rk, 'owner_id') or ''
  local lease_ms = tonumber(redis.call('HGET', rk, 'lease_ms')) or 0
  local fencing = redis.call('INCR', k.fence)
  local token = new_token(owner_id, req_id, fencing)
  local expire_at = now + lease_ms
  redis.call('HSET', k.writer, 'token', token, 'expire_at_ms', expire_at)
  redis.call('PEXPIRE', k.writer, lease_ms)
  redis.call('HSET', rk, 'granted_token', token)
  push_grant(prefix, req_id, token, fencing, expire_at, 'write', notify_ttl, now)
  redis.call('ZREM', k.queue, req_id)
  return token
end

-- Grant a single queued reader (its req hash exists).
local function grant_reader(k, prefix, id, now, notify_ttl)
  local rk = req_key(prefix, id)
  local owner_id = redis.call('HGET', rk, 'owner_id') or ''
  local lease_ms = tonumber(redis.call('HGET', rk, 'lease_ms')) or 0
  local fencing = redis.call('INCR', k.fence)
  local token = new_token(owner_id, id, fencing)
  local expire_at = now + lease_ms
  redis.call('ZADD', k.readers, expire_at, token)
  redis.call('HSET', rk, 'granted_token', token)
  push_grant(prefix, id, token, fencing, expire_at, 'read', notify_ttl, now)
  redis.call('ZREM', k.queue, id)
end

-- Grant consecutive readers from the head, stopping at the first queued writer and
-- after that reader's max_reader_batch grants (write_preferring / fifo).
local function grant_contiguous_readers(k, prefix, now, notify_ttl)
  local count = 0
  while true do
    local head = redis.call('ZRANGE', k.queue, 0, 0)
    if #head == 0 then break end
    local id = head[1]
    local rk = req_key(prefix, id)
    if redis.call('HGET', rk, 'mode') ~= 'read' then break end
    local mrb = tonumber(redis.call('HGET', rk, 'max_reader_batch')) or 1000
    if count >= mrb then break end
    grant_reader(k, prefix, id, now, notify_ttl)
    count = count + 1
  end
  return count
end

-- Grant every queued reader in FIFO order, SKIPPING queued writers (read_preferring).
local function grant_readers_anywhere(k, prefix, now, notify_ttl)
  local members = redis.call('ZRANGE', k.queue, 0, -1)
  local count = 0
  for _, id in ipairs(members) do
    local rk = req_key(prefix, id)
    if redis.call('HGET', rk, 'mode') == 'read' then
      local mrb = tonumber(redis.call('HGET', rk, 'max_reader_batch')) or 1000
      if count >= mrb then break end
      grant_reader(k, prefix, id, now, notify_ttl)
      count = count + 1
    end
  end
  return count
end

-- The heart of the system: decide who, if anyone, is woken next. Governing policy is
-- the head request's `fairness` (expected uniform per resource).
local function grant_from_queue(k, prefix, now, notify_ttl)
  sweep(k, now)
  prune_queue(k, prefix, now)
  if writer_held(k) then return 0 end

  local head = redis.call('ZRANGE', k.queue, 0, 0)
  if #head == 0 then return 0 end
  local head_id = head[1]
  local head_rk = req_key(prefix, head_id)
  local fairness = redis.call('HGET', head_rk, 'fairness') or 'write_preferring'

  if reader_count(k) > 0 then
    if fairness == 'read_preferring' then
      return grant_readers_anywhere(k, prefix, now, notify_ttl)
    end
    return grant_contiguous_readers(k, prefix, now, notify_ttl)
  end

  if fairness == 'read_preferring' then
    local g = grant_readers_anywhere(k, prefix, now, notify_ttl)
    if g > 0 then return g end
    if redis.call('HGET', head_rk, 'mode') == 'write' then
      grant_writer(k, prefix, head_id, now, notify_ttl); return 1
    end
    return 0
  end

  if redis.call('HGET', head_rk, 'mode') == 'write' then
    grant_writer(k, prefix, head_id, now, notify_ttl); return 1
  end
  return grant_contiguous_readers(k, prefix, now, notify_ttl)
end
