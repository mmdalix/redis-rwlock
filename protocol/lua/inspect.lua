-- inspect.lua — read-only debug snapshot (SPEC §18). Registered as a no-writes
-- function; performs NO writes, computing liveness directly so the report is accurate
-- even with a pending sweep.
--
-- KEYS[1] = prefix
-- Returns (positional): mode("none"|"read"|"write"), readerCount, writerActive(0|1),
--   queueLength, queuedWriters, oldestWaitMs(-1 if none), nextExpiryMs(-1 if none)

local prefix = KEYS[1]
local k = keys_for(prefix)

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

-- live writer?
local writer_active = 0
local next_expiry = -1
if redis.call('EXISTS', k.writer) == 1 then
  local we = tonumber(redis.call('HGET', k.writer, 'expire_at_ms'))
  if we and we > now then
    writer_active = 1
    next_expiry = we
  end
end

-- live readers (score > now), without evicting
local reader_count = tonumber(redis.call('ZCOUNT', k.readers, '(' .. now, '+inf')) or 0
if reader_count > 0 then
  local r = redis.call('ZRANGEBYSCORE', k.readers, '(' .. now, '+inf', 'WITHSCORES', 'LIMIT', 0, 1)
  if #r >= 2 then
    local rs = tonumber(r[2])
    if next_expiry == -1 or rs < next_expiry then next_expiry = rs end
  end
end

-- queue stats
local members = redis.call('ZRANGE', k.queue, 0, -1)
local queue_length = 0
local queued_writers = 0
local oldest_wait = -1
for _, id in ipairs(members) do
  local rk = req_key(prefix, id)
  local wd = redis.call('HGET', rk, 'wait_deadline_ms')
  local gt = redis.call('HGET', rk, 'granted_token')
  if (not is_blank(wd)) and tonumber(wd) > now and is_blank(gt) then
    queue_length = queue_length + 1
    if redis.call('HGET', rk, 'mode') == 'write' then queued_writers = queued_writers + 1 end
    local ca = tonumber(redis.call('HGET', rk, 'created_at_ms'))
    if ca then
      local waited = now - ca
      if waited > oldest_wait then oldest_wait = waited end
    end
  end
end

local mode = 'none'
if writer_active == 1 then mode = 'write' elseif reader_count > 0 then mode = 'read' end

local next_expiry_ms = -1
if next_expiry ~= -1 then next_expiry_ms = next_expiry - now end

return { mode, reader_count, writer_active, queue_length, queued_writers, oldest_wait, next_expiry_ms }
