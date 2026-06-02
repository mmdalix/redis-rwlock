-- lib.lua — shared helpers for all redis-rwlock scripts.
--
-- This file is NOT loaded on its own. The client's generator (gen:lua) prepends
-- it to every individual script (acquire/release/extend/cancel_wait/expire_and_grant)
-- so the helpers below are in scope. When delivered as a Redis FUNCTION library
-- (later milestone) these become library-local functions instead. Either way the
-- source of truth is here and nowhere else.
--
-- Conventions:
--   * KEYS[1] is the per-resource prefix "rwlock:{<resource>}" (carries the cluster
--     hash tag), and every key we touch is derived from it, so a script only ever
--     touches keys in one slot.
--   * Time is always read from the Redis server via TIME (never passed by clients).
--   * The `state` hash is a denormalized cache kept in lockstep with the `holders`
--     ZSET (the single source of truth) inside these atomic scripts.

local function now_ms()
  local t = redis.call('TIME')                       -- { seconds, microseconds }
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

-- Derive the fixed per-resource keys from the prefix in KEYS[1].
local function keys_for(prefix)
  return {
    state        = prefix .. ':state',
    holders      = prefix .. ':holders',
    holder_meta  = prefix .. ':holder_meta',
    queue        = prefix .. ':queue',
    seq          = prefix .. ':seq',
    fence        = prefix .. ':fence',
    -- A TTL'd sentinel mirroring the soonest holder lease expiry. Its only purpose
    -- is to make holder-lease expiry observable via Redis keyspace notifications
    -- (holders live in a ZSET by score and otherwise fire no expired event). The
    -- core grant logic never reads it; it is an opt-in recovery accelerator (§10.3).
    lease_expiry = prefix .. ':lease_expiry',
  }
end

local function req_key(prefix, id)    return prefix .. ':req:' .. id end
local function notify_key(prefix, id) return prefix .. ':notify:' .. id end

local function is_blank(v)
  return v == nil or v == false or v == ''
end

-- Arm (or clear) the lease-expiry sentinel to fire a keyspace `expired` event at the
-- soonest current holder's lease boundary. Called at the end of every mutating script.
local function arm_lease_sentinel(k, now)
  local mn = redis.call('ZRANGE', k.holders, 0, 0, 'WITHSCORES')
  if #mn >= 2 then
    local ms = tonumber(mn[2]) - now
    if ms < 1 then ms = 1 end
    redis.call('SET', k.lease_expiry, '1', 'PX', ms)
  else
    redis.call('DEL', k.lease_expiry)
  end
end

-- Recompute mode / writer_token / reader_count from the holders ZSET. Does NOT
-- touch queued_writers (that counter is owned by enqueue/grant/cancel paths).
local function recompute_state_cache(k)
  local members = redis.call('ZRANGE', k.holders, 0, -1)
  local writer = ''
  local readers = 0
  for _, tok in ipairs(members) do
    local mj = redis.call('HGET', k.holder_meta, tok)
    if mj then
      local m = cjson.decode(mj)
      if m.mode == 'write' then
        writer = tok
      else
        readers = readers + 1
      end
    end
  end
  if writer ~= '' then
    redis.call('HSET', k.state, 'mode', 'write', 'writer_token', writer, 'reader_count', 0)
  elseif #members > 0 then
    redis.call('HSET', k.state, 'mode', 'read', 'writer_token', '', 'reader_count', readers)
  else
    redis.call('HSET', k.state, 'mode', 'none', 'writer_token', '', 'reader_count', 0)
  end
end

-- Evict expired holders (score = expire_at_ms <= now), drop their meta, refresh cache.
local function sweep(k, now)
  local expired = redis.call('ZRANGEBYSCORE', k.holders, 0, now)
  if #expired > 0 then
    for _, tok in ipairs(expired) do
      redis.call('HDEL', k.holder_meta, tok)
    end
    redis.call('ZREMRANGEBYSCORE', k.holders, 0, now)
  end
  recompute_state_cache(k)
end

local function dec_queued_writers(k)
  local qw = tonumber(redis.call('HGET', k.state, 'queued_writers')) or 0
  if qw > 0 then redis.call('HSET', k.state, 'queued_writers', qw - 1) end
end

-- Prune the queue of orphaned and timed-out requests, and recompute queued_writers
-- from the surviving LIVE requests (never a request already granted at the buzzer,
-- which still carries granted_token and is reconciled elsewhere).
--
-- queued_writers MUST be derived from the source of truth (the live req hashes),
-- not maintained as a pure incremental counter: an orphaned queue entry (a waiter
-- that crashed and whose req hash TTL-expired) cannot report its mode, so an
-- incremental counter would drift up permanently on a crashed queued writer and —
-- under write_preferring — starve every subsequent reader forever.
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
  redis.call('HSET', k.state, 'queued_writers', writers)
end

local function push_grant(prefix, req_id, token, fencing, expire_at, mode, notify_key_ttl_ms)
  local payload = cjson.encode({
    status = 'GRANTED',
    token = token,
    fencing = fencing,
    lease_until_ms = expire_at,
    mode = mode,
  })
  local nk = notify_key(prefix, req_id)
  redis.call('LPUSH', nk, payload)
  redis.call('PEXPIRE', nk, notify_key_ttl_ms)
end

-- Grant the writer at request `req_id` (its hash already exists in the queue).
local function grant_one_writer(k, prefix, req_id, now, notify_key_ttl_ms)
  local rk = req_key(prefix, req_id)
  local owner_id = redis.call('HGET', rk, 'owner_id') or ''
  local lease_ms = tonumber(redis.call('HGET', rk, 'lease_ms')) or 0
  local fencing = redis.call('INCR', k.fence)
  local token = owner_id .. ':' .. req_id .. ':' .. fencing
  local expire_at = now + lease_ms
  redis.call('ZADD', k.holders, expire_at, token)
  redis.call('HSET', k.holder_meta, token, cjson.encode({
    mode = 'write', owner_id = owner_id, fencing = fencing, request_id = req_id }))
  redis.call('HSET', k.state, 'mode', 'write', 'writer_token', token, 'reader_count', 0)
  redis.call('HSET', rk, 'granted_token', token)
  dec_queued_writers(k)
  push_grant(prefix, req_id, token, fencing, expire_at, 'write', notify_key_ttl_ms)
  redis.call('ZREM', k.queue, req_id)
  return token
end

-- Grant a single queued reader (its req hash already exists). Adds it to holders,
-- writes meta + granted_token, pushes the grant, and removes it from the queue.
local function grant_one_reader(k, prefix, id, now, notify_key_ttl_ms)
  local rk = req_key(prefix, id)
  local owner_id = redis.call('HGET', rk, 'owner_id') or ''
  local lease_ms = tonumber(redis.call('HGET', rk, 'lease_ms')) or 0
  local fencing = redis.call('INCR', k.fence)
  local token = owner_id .. ':' .. id .. ':' .. fencing
  local expire_at = now + lease_ms
  redis.call('ZADD', k.holders, expire_at, token)
  redis.call('HSET', k.holder_meta, token, cjson.encode({
    mode = 'read', owner_id = owner_id, fencing = fencing, request_id = id }))
  redis.call('HSET', rk, 'granted_token', token)
  push_grant(prefix, id, token, fencing, expire_at, 'read', notify_key_ttl_ms)
  redis.call('ZREM', k.queue, id)
end

-- Grant consecutive readers from the head of the queue, stopping at the first
-- queued writer (so a writer is never starved past its position) and after
-- max_reader_batch grants. Used by write_preferring / fifo. Returns the count.
local function grant_contiguous_readers(k, prefix, now, notify_key_ttl_ms)
  local count = 0
  while true do
    local head = redis.call('ZRANGE', k.queue, 0, 0)
    if #head == 0 then break end
    local id = head[1]
    local rk = req_key(prefix, id)
    if redis.call('HGET', rk, 'mode') ~= 'read' then break end   -- stop at a queued writer
    local mrb = tonumber(redis.call('HGET', rk, 'max_reader_batch')) or 1000
    if count >= mrb then break end
    grant_one_reader(k, prefix, id, now, notify_key_ttl_ms)
    count = count + 1
  end
  if count > 0 then recompute_state_cache(k) end
  return count
end

-- Grant every queued reader in FIFO order, SKIPPING over queued writers (so
-- readers may jump ahead of a waiting writer). Used only by read_preferring,
-- which maximizes read throughput at the cost of possible writer starvation.
-- Capped by max_reader_batch to bound script runtime.
local function grant_readers_anywhere(k, prefix, now, notify_key_ttl_ms)
  local members = redis.call('ZRANGE', k.queue, 0, -1)   -- ordered snapshot
  local count = 0
  for _, id in ipairs(members) do
    local rk = req_key(prefix, id)
    if redis.call('HGET', rk, 'mode') == 'read' then
      local mrb = tonumber(redis.call('HGET', rk, 'max_reader_batch')) or 1000
      if count >= mrb then break end
      grant_one_reader(k, prefix, id, now, notify_key_ttl_ms)
      count = count + 1
    end
  end
  if count > 0 then recompute_state_cache(k) end
  return count
end

-- The heart of the system: decide who, if anyone, gets woken next. The governing
-- fairness policy is the head request's (fairness is expected to be uniform per
-- resource namespace).
local function grant_from_queue(k, prefix, now, notify_key_ttl_ms)
  sweep(k, now)
  prune_queue(k, prefix, now)

  local writer_token = redis.call('HGET', k.state, 'writer_token')
  if not is_blank(writer_token) then return 0 end          -- a writer holds -> nobody else

  local head = redis.call('ZRANGE', k.queue, 0, 0)
  if #head == 0 then return 0 end
  local head_id = head[1]
  local head_rk = req_key(prefix, head_id)
  local fairness = redis.call('HGET', head_rk, 'fairness') or 'write_preferring'
  local reader_count = tonumber(redis.call('HGET', k.state, 'reader_count')) or 0

  if reader_count > 0 then
    -- readers currently hold: only MORE readers can join.
    if fairness == 'read_preferring' then
      return grant_readers_anywhere(k, prefix, now, notify_key_ttl_ms)   -- jump queued writers
    end
    -- write_preferring / fifo: only contiguous readers at the head (stops at a
    -- queued writer, granting nobody and draining the readers first).
    return grant_contiguous_readers(k, prefix, now, notify_key_ttl_ms)
  end

  -- no holders at all.
  if fairness == 'read_preferring' then
    local granted = grant_readers_anywhere(k, prefix, now, notify_key_ttl_ms)
    if granted > 0 then return granted end
    -- no readers queued -> the head must be a writer; serve it.
    if redis.call('HGET', head_rk, 'mode') == 'write' then
      grant_one_writer(k, prefix, head_id, now, notify_key_ttl_ms)
      return 1
    end
    return 0
  end

  -- write_preferring / fifo: serve strictly from the head.
  if redis.call('HGET', head_rk, 'mode') == 'write' then
    grant_one_writer(k, prefix, head_id, now, notify_key_ttl_ms)
    return 1
  end
  return grant_contiguous_readers(k, prefix, now, notify_key_ttl_ms)
end
