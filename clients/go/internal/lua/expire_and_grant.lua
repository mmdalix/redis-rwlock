-- GENERATED from protocol/lua/expire_and_grant.lua — DO NOT EDIT (run tools/sync-lua).
-- expire_and_grant.lua — maintenance: evict expired holders/requests, then grant.
--
-- KEYS[1] = prefix
-- ARGV    = notify_key_ttl_ms
-- Returns: { "OK", granted_count, next_wake_ms }
--
-- Used by the per-waiter self-wake path and the optional keyspace-expiry subscriber.
-- It never acquires for a caller; it frees expired capacity and lets grant_from_queue
-- wake whoever is eligible via their mailboxes. next_wake_ms lets a waiter refresh its
-- self-wake boundary after running maintenance.

local prefix = KEYS[1]
local k = keys_for(prefix)
local notify_ttl = tonumber(ARGV[1])

local now = now_ms()
local granted = grant_from_queue(k, prefix, now, notify_ttl)
return { 'OK', granted, next_wake(k, prefix, now) }
