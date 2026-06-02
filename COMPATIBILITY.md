# Compatibility

## Protocol versions

`PROTOCOL_VERSION` is a single integer embedded in the shared Lua and written to
`rwlock:__module__` on install. **Compatibility is exact-integer equality** — clients
with the same `PROTOCOL_VERSION` are guaranteed identical semantics and are safe to
contend on the same Redis; clients with different versions MUST NOT share a resource
namespace, and a client raises `IncompatibleServerLogic` (Node: `IncompatibleServerLogicError`)
when it detects a different version installed (unless `allowIncompatibleProtocol` is set).

| PROTOCOL_VERSION | Status | Notes |
|---|---|---|
| **2** | current | Source-of-truth data model: separate `readers` ZSET + TTL'd `writer` key (no denormalized state cache, no per-holder metadata, no `lease_expiry` sentinel). `queued_writers` derived from the live queue (drift-free). `extend` never shortens. Mailbox TTL ≥ lease. `next_wake` self-wake covers crashed queued heads. Server-side limit clamping. |
| 1 | superseded (pre-release) | `holders` ZSET + `holder_meta` + denormalized `state` cache + `lease_expiry` sentinel. Had a `queued_writers` drift bug (crashed queued writer → reader starvation) and the §10.3 recovery layer was unimplementable against its key schema. Never published. |

v1 and v2 are **incompatible** (key schema + grant algorithm changed). Since v1 was never
published, no migration path is required.

## Client ⇄ protocol

| Client | Package | Speaks |
|---|---|---|
| Node.js | `@org/redis-rwlock` (`clients/node`) | PROTOCOL_VERSION 2 |

Each client publishes on its own SemVer; a **client-only** change (language bug fix /
ergonomics) bumps just that client, while any change to `protocol/lua`, the key schema,
a script return contract, or grant semantics bumps `PROTOCOL_VERSION` and requires a
coordinated release of every client.

## Redis

Works against standalone, Sentinel, and Cluster Redis (the `{resource}` hash tag keeps a
resource's keys on one slot). Redis Functions are used when `FUNCTION LOAD` is available,
falling back transparently to `EVALSHA`. Requires Redis ≥ 7 for the FUNCTION path; the
`EVALSHA` path works on older servers. Live multi-node Cluster *client* integration
(per-node FUNCTION LOAD, per-node blocking pools) is in progress.
