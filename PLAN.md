# Implementation Plan — redis-rwlock

**Reference client:** Node.js / TypeScript (wraps `node-redis` v5/v6). Go/Python follow as ports.

## Guiding principle

The Lua scripts in `protocol/lua/` are the product; clients are thin wrappers
(Spec §0, §3). We build a **thin vertical slice** — Lua + the Node client + a
shared conformance harness — and grow it milestone by milestone. We deliberately
do **not** scaffold the full polyglot tree, the `sync-lua`/`check-sync` tooling, or
the CI interop matrix until a second language actually exists (milestone M7); until
then they are empty cost.

## Repo layout (current slice)

```
SPEC.md                              # source of truth (the spec)
PLAN.md                              # this file
protocol/
  VERSION                            # PROTOCOL_VERSION = 1
  lua/                               # the ONLY hand-edited Lua
    lib.lua                          # shared helpers + grant_from_queue
    acquire.lua release.lua extend.lua cancel_wait.lua expire_and_grant.lua
  conformance/
    README.md  scenarios/*.json      # language-agnostic scenarios (Spec §20.10)
clients/node/                        # node-redis wrapper + generated Lua + tests
  scripts/gen-lua.mjs                # embeds protocol/lua -> src/lua.generated.ts
  src/  test/
```

Deferred to M7 (when a 2nd language lands): `clients/` vs top-level layout decision,
`tools/sync-lua` + `check-sync` drift guard, `COMPATIBILITY.md`, the CI matrix.

## Milestones (each ends with its slice of Spec §20 green)

| ID | Scope | Status |
|----|-------|--------|
| **M0** | Single-node **write lock**: acquire/release, lease, token verify, fencing, `BLPOP` mailbox, fail-closed, server `TIME`. Plus the queue + `cancel_wait` + wait loop needed for a correct timeout/handoff path, and a bounded self-wake for crash recovery. | ✅ in this slice |
| M1 | **Read locks** + `grant_contiguous_readers` batching + `state` cache. Concurrent co-holding, single-release batch grant, stop-at-queued-writer (§6.2), `max_reader_batch` cap, state-cache accuracy — all tested. | ✅ done |
| M2 | FIFO **queue** + all three `fairness` policies + `cancel_wait` ghost-grant reconciliation. read_preferring (readers jump writers), fifo & write_preferring (no writer starvation), and the timeout↔grant race (§20.5, stress-tested) all proven. | ✅ done |
| M3 | `extend` safety margin + opt-in **watchdog** + **scoped API** (`withWriteLock`/`withReadLock`) with `AbortSignal` cancellation. | `extend` done; scoped/watchdog TODO |
| M4 | **Recovery**: lazy cleanup (done) + self-wake (done) + optional keyspace-event subscriber. | self-wake done |
| M5 | **Cluster** hash-tagging + **Functions-or-EVALSHA** delivery + capability probe + **version handshake** (`rwlock:__module__`). | EVALSHA done; rest TODO |
| M6 | **Observability** (metrics/tracing/`inspect`) + dedicated blocking-connection **pool** + backpressure. | TODO |
| M7 | **Second language port** over the same Lua + **cross-language conformance** + full repo structure & CI. | TODO |

## What this first slice (M0) delivers and proves

- Atomic server-side write-lock state machine in Lua (acquire / release /
  `grant_from_queue` / `cancel_wait` / `expire_and_grant`), plus `extend`.
- Node client: raw `acquireWrite`/`acquireRead`/`release`/`extend`, the `BLPOP`
  mailbox wait loop with bounded self-wake, fencing, fail-closed errors, server-time
  offset for sizing timeouts, dedicated blocking connection per wait.
- Tests against a real `redis-server`: uncontended grant, mutual exclusion + timeout,
  immediate handoff on release, monotonic fencing, crash recovery (self-wake + lazy),
  fail-closed, extend/LOST, plus read-lock smoke tests and the shared conformance runner.

## Not yet (called out so nothing is over-claimed)

Scoped/closure API + watchdog, extend safety-margin guard, keyspace-event subscriber,
Cluster routing, Redis Functions delivery + version handshake, connection pooling/
backpressure, metrics/tracing/`inspect`, and the second-language port. See the table.
