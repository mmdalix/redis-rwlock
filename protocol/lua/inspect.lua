-- inspect.lua — read-only debug snapshot of a resource (SPEC §18). Performs NO
-- writes (registered as a no-writes function), so it is safe on replicas and never
-- mutates state. Computes liveness directly from the holders ZSET rather than the
-- denormalized cache, so the report is accurate even if a sweep is pending.
--
-- KEYS[1] = prefix
-- Returns (positional): mode, readerCount, writerActive(0|1), queueLength,
--                       queuedWriters, oldestWaitMs(-1 if none), nextExpiryMs(-1 if none)

local prefix = KEYS[1]
local k = keys_for(prefix)

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local members = redis.call('ZRANGE', k.holders, 0, -1, 'WITHSCORES')
local reader_count = 0
local writer_active = 0
local min_score = -1
for i = 1, #members, 2 do
  local tok = members[i]
  local score = tonumber(members[i + 1])
  if score > now then
    if min_score == -1 or score < min_score then min_score = score end
    local mj = redis.call('HGET', k.holder_meta, tok)
    if mj then
      local m = cjson.decode(mj)
      if m.mode == 'write' then writer_active = 1 else reader_count = reader_count + 1 end
    end
  end
end

local queue_length = redis.call('ZCARD', k.queue)
local queued_writers = tonumber(redis.call('HGET', k.state, 'queued_writers')) or 0

local oldest_wait_ms = -1
local head = redis.call('ZRANGE', k.queue, 0, 0)
if #head > 0 then
  local ca = redis.call('HGET', req_key(prefix, head[1]), 'created_at_ms')
  if ca then oldest_wait_ms = now - tonumber(ca) end
end

local next_expiry_ms = -1
if min_score ~= -1 then next_expiry_ms = min_score - now end

local mode = 'none'
if writer_active == 1 then mode = 'write' elseif reader_count > 0 then mode = 'read' end

return { mode, reader_count, writer_active, queue_length, queued_writers, oldest_wait_ms, next_expiry_ms }
