# Compatibility

## Protocol versions

`PROTOCOL_VERSION` is a single integer embedded in the shared Lua and written to
`rwlock:__module__` on install. **Compatibility is exact-integer equality** — clients
with the same `PROTOCOL_VERSION` are guaranteed identical semantics and are safe to
contend on the same Redis; clients with a different version MUST NOT share a resource
namespace, and a client raises `IncompatibleServerLogic` (Node: `IncompatibleServerLogicError`)
when it detects a different version installed (unless `allowIncompatibleProtocol` is set).

| PROTOCOL_VERSION | Status | Notes |
|---|---|---|
| **1** | current (unreleased) | State derived from source-of-truth structures: a `readers` ZSET + a TTL'd `writer` key (no denormalized state cache, no per-holder metadata). `queued_writers` derived from the live queue (drift-free). Crashed writers self-expire via the writer key's native TTL. `extend` never shortens. Mailbox TTL ≥ lease. `next_wake` self-wake covers crashed queued heads. Limits clamped server-side. |

Nothing has been published yet, so the protocol is still being edited in place; the
version stays `1` until the first release. Any change to `protocol/lua`, the key schema,
a script return contract, or grant semantics that ships **after** a release will bump
`PROTOCOL_VERSION`.

## Client ⇄ protocol

| Client | Package | Speaks |
|---|---|---|
| Node.js | `@org/redis-rwlock` (`clients/node`) | PROTOCOL_VERSION 1 |

Each client publishes on its own SemVer; a **client-only** change (language bug fix /
ergonomics) bumps just that client, while a **protocol** change bumps `PROTOCOL_VERSION`
and requires a coordinated release of every client.

## Redis

Works against standalone, Sentinel, and Cluster Redis (the `{resource}` hash tag keeps a
resource's keys on one slot). Redis Functions are used when `FUNCTION LOAD` is available,
falling back transparently to `EVALSHA`. Requires Redis ≥ 7 for the FUNCTION path; the
`EVALSHA` path works on older servers. Live multi-node Cluster *client* integration
(per-node FUNCTION LOAD, per-node blocking pools) is in progress.
