-- expire_and_grant.lua — pure maintenance: evict expired holders/requests, then grant.
--
-- KEYS[1] = prefix
-- ARGV    = notify_key_ttl_ms
--
-- Returns: { "OK", granted_count }
--
-- Used by the per-waiter self-wake path (and, later, the optional keyspace-expiry
-- subscriber). It never acquires *for* a caller; it just frees expired capacity and
-- lets grant_from_queue wake whoever is eligible via their own mailboxes.

local prefix = KEYS[1]
local k = keys_for(prefix)
local notify_ttl = tonumber(ARGV[1])

local now = now_ms()
local granted = grant_from_queue(k, prefix, now, notify_ttl)   -- sweep + drop happen inside
arm_lease_sentinel(k, now)
return { 'OK', granted }
