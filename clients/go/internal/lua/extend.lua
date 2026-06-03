-- extend.lua — renew the calling holder's lease (never shortens it).
--
-- KEYS[1] = prefix
-- ARGV    = token, lease_ms
-- Returns: { "OK", new_lease_until_ms } | { "LOST" }
--
-- The "extend only within a safety margin" rule is enforced client-side; this script
-- only verifies liveness and renews. It never moves the expiry earlier (GT semantics),
-- so passing a shorter lease cannot accidentally drop the lock.

local prefix = KEYS[1]
local k = keys_for(prefix)
local token = ARGV[1]
local lease_ms = clamp(tonumber(ARGV[2]) or 0, 1, MAX_LEASE_MS)

local now = now_ms()

if redis.call('EXISTS', k.writer) == 1 and redis.call('HGET', k.writer, 'token') == token then
  local cur = tonumber(redis.call('HGET', k.writer, 'expire_at_ms'))
  if cur == nil or cur <= now then return { 'LOST' } end
  local new_expire = now + lease_ms
  if new_expire < cur then new_expire = cur end
  redis.call('HSET', k.writer, 'expire_at_ms', new_expire)
  redis.call('PEXPIRE', k.writer, new_expire - now)
  return { 'OK', new_expire }
end

local score = redis.call('ZSCORE', k.readers, token)
if score and tonumber(score) > now then
  local new_expire = now + lease_ms
  if new_expire < tonumber(score) then new_expire = tonumber(score) end
  redis.call('ZADD', k.readers, new_expire, token)
  return { 'OK', new_expire }
end

return { 'LOST' }
