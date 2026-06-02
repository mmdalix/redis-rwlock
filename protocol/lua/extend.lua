-- extend.lua — renew the calling holder's lease (liveness check only).
--
-- KEYS[1] = prefix
-- ARGV    = token, lease_ms
--
-- Returns: { "OK", new_lease_until_ms } | { "LOST" }
--
-- The "extend only within a safety margin" rule is enforced client-side before
-- calling; this script only verifies the token is still a live holder.

local prefix = KEYS[1]
local k = keys_for(prefix)
local token = ARGV[1]
local lease_ms = tonumber(ARGV[2])

local now = now_ms()
local meta = redis.call('HGET', k.holder_meta, token)
local score = redis.call('ZSCORE', k.holders, token)
if is_blank(meta) or score == false or score == nil or tonumber(score) <= now then
  return { 'LOST' }
end

local new_expire = now + lease_ms
redis.call('ZADD', k.holders, new_expire, token)
arm_lease_sentinel(k, now)
return { 'OK', new_expire }
