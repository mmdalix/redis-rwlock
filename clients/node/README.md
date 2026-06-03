# redis-rwlock

[![npm version](https://img.shields.io/npm/v/redis-rwlock.svg)](https://www.npmjs.com/package/redis-rwlock)
[![CI](https://github.com/mmdalix/redis-rwlock/actions/workflows/ci.yml/badge.svg)](https://github.com/mmdalix/redis-rwlock/actions/workflows/ci.yml)
[![types: included](https://img.shields.io/badge/types-included-blue.svg)](https://github.com/mmdalix/redis-rwlock)
[![license: MIT](https://img.shields.io/badge/license-MIT-green.svg)](./LICENSE)
[![node: >=22](https://img.shields.io/badge/node-%3E%3D22-brightgreen.svg)](#requirements)

**A distributed read/write lock for Redis that's honest about correctness.** Many
readers share a resource, or exactly one writer holds it — with FIFO fair queueing
(no polling), instant hand-off, leases, a watchdog, crash recovery, and a **fencing
token on every acquire** (the thing most Redis locks quietly omit).

All the lock logic runs as **atomic, server-side Redis scripts** (a Redis FUNCTION
library, with an automatic `EVALSHA` fallback). The client is a thin, typed wrapper
over your existing [`node-redis`](https://github.com/redis/node-redis) connection —
so semantics are identical everywhere, and there's no connection layer fighting yours.

```ts
await rw.withWriteLock("order:123", { leaseMs: 30_000, watchdog: true }, async (lock) => {
  await chargeCard(amount, { fencingToken: lock.fencingToken }); // safe even across pauses
});
// acquired, auto-extended while held, and released — even if the callback throws
```

## Why redis-rwlock?

|  | **redis-rwlock** | Redlock | `etcd`/ZooKeeper | in-process `Mutex` |
|---|:---:|:---:|:---:|:---:|
| Read **and** write modes (shared readers) | ✅ | ❌ | ⚠️ build it | ✅ (RWMutex) |
| Fair FIFO queueing, **no polling** (direct hand-off) | ✅ | ❌ (retry loops) | ✅ | ✅ |
| **Fencing token** on every grant | ✅ | ❌ | ✅ | n/a |
| Lease + crash recovery, **zero background infra** | ✅ | ✅ | ✅ | n/a |
| Scoped API + watchdog + `AbortSignal` + `await using` | ✅ | ⚠️ varies | ⚠️ | ✅ |
| Runs on your existing Redis (standalone/Sentinel/Cluster) | ✅ | ✅ | separate cluster | n/a |

If you already run Redis and want a *correct-when-fenced*, ergonomic RW lock,
this is the pragmatic choice.

## Features

- 🔁 **Read/write locks** — many readers together, or one exclusive writer.
- 🎟️ **Fencing tokens** — a monotonic `fencingToken` on every acquire, the real
  correctness boundary (see [below](#is-it-safe)).
- 🚦 **FIFO fair queueing, no polling** — waiters block on a private mailbox and are
  *handed* the lock the instant it frees. Configurable `write_preferring` / `fifo` /
  `read_preferring` fairness; no retry storms, no cluster-wide fan-out.
- ⏱️ **Leases + watchdog** — every hold expires (so a crash never deadlocks); opt-in
  watchdog auto-extends long operations.
- 🧯 **Crash recovery, zero infra** — lazy cleanup + per-waiter self-wake + optional
  keyspace-event acceleration. No dispatcher, no `CONFIG SET`, nothing to operate.
- 🧩 **Ergonomic API** — scoped `withWriteLock`/`withReadLock` with guaranteed release,
  an `AbortSignal` that fires the moment the lock is lost, and `await using` support.
- 🧠 **One brain** — all logic in shared Lua, delivered as a Redis FUNCTION library
  (or `EVALSHA`), so behavior is identical across clients and a version handshake stops
  incompatible peers from contending.
- 📈 **Observable** — `inspect()` plus pluggable metrics/tracing sinks (Prometheus /
  OpenTelemetry / StatsD).
- 🪶 **Thin & typed** — wraps your `node-redis` client, ships dual ESM/CJS with
  first-class TypeScript types.

## Install

```bash
npm install redis-rwlock redis
```

`redis` (node-redis v5 or v6) is a peer dependency — bring your own connected client.

## Quickstart

The **scoped API** is the front door: guaranteed release, cancellation tied to lock
liveness, and (with `watchdog`) auto-extension.

```ts
import { createClient } from "redis";
import { RwLock } from "redis-rwlock";

const client = await createClient().connect();
const rw = new RwLock(client);

// Write lock — exclusive
await rw.withWriteLock("order:123", { leaseMs: 30_000, watchdog: true }, async (lock) => {
  // `lock.signal` aborts the instant the lease is lost — thread it through your work
  await doWork({ signal: lock.signal });
  await storage.write(payload, { fencingToken: lock.fencingToken }); // enforce fencing
});

// Read lock — shared
const total = await rw.withReadLock("order:123", async () => sumLineItems());
```

Power-user form with `await using` (auto-release at scope end):

```ts
await using lock = await rw.acquireWrite("order:123");
await mutate(lock.fencingToken);
// released automatically
```

Call `await rw.close()` when you're done with the instance (it releases its internal
blocking-connection pool and never touches your client).

## Is it safe?

> **Read once — 30 seconds.** Like **every** distributed lock built on Redis —
> **including Redlock** — this is a *lease* (a lock with a TTL), not a linearizable
> lock. A holder that pauses (GC, scheduling, VM migration) past its lease can have the
> lock reassigned while it still thinks it holds it. **No Redis-side trick removes
> this** — it's [inherent to distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html),
> not a quirk of this library.
>
> The fix is a **fencing token**, and this is where `redis-rwlock` goes *further* than
> most: **every acquire returns a monotonic `fencingToken`** — plain Redlock doesn't.
>
> - **For efficiency** (dedupe work, reduce contention, single-flight a job/cache
>   rebuild) → safe as-is, like any lock.
> - **For correctness** (a double-grant corrupts data or moves money) → enforce the
>   token at your storage layer in one line: reject any write whose `fencingToken` is
>   `≤` the highest you've already accepted for that resource.
>
> Need linearizability and can't do a resource-side check? Use a consensus system
> (etcd / ZooKeeper / Consul). For everything else, a *fenced* Redis lock is the
> pragmatic, fast choice — and this one hands you the fence.

## API

```ts
const rw = new RwLock(client, config?);

// Scoped (recommended) — guaranteed release + cancellation + optional watchdog
rw.withWriteLock(resource, opts?, async (lock) => T): Promise<T>
rw.withReadLock(resource, opts?, async (lock) => T): Promise<T>

// Raw — LockHandle is AsyncDisposable (works with `await using`)
rw.acquireWrite(resource, opts?): Promise<LockHandle>
rw.acquireRead(resource, opts?): Promise<LockHandle>
rw.release(handle): Promise<void>
rw.extend(handle, leaseMs?): Promise<LockHandle>   // never shortens; throws LockLostError if lost

rw.inspect(resource): Promise<ResourceStatus>      // debug snapshot
rw.close(): Promise<void>

// LockHandle: { resource, mode, token, fencingToken, leaseUntilMs, signal }
```

### Acquire options

| Option | Default | Notes |
|---|---|---|
| `leaseMs` | 30000 (max 300000) | how long you may hold it |
| `waitMs` | 10000 (max 60000) | how long to block waiting |
| `fairness` | `write_preferring` | `write_preferring` \| `fifo` \| `read_preferring` |
| `watchdog` | `false` | auto-extend at ~lease/3 while held |
| `ownerId` | `<hostname>#<pid>` | "who" holds it, for `inspect`/logs (optional) |
| `signal` | — | `AbortSignal` to cancel a pending acquire |
| `maxReaderBatch` | 1000 | cap readers woken by one grant |

`new RwLock(client, config)` also takes `metrics`/`tracer` sinks, `blockingPoolSize`,
`keyspaceEvents`, `defaultLeaseMs`, and more.

### Fairness

- **`write_preferring`** (default) — a queued writer blocks *new* readers from jumping
  ahead; existing readers drain, then the writer goes. Mirrors Go's `sync.RWMutex`.
- **`fifo`** — strict queue order (contiguous readers still batch).
- **`read_preferring`** — readers proceed whenever no writer holds; max read throughput,
  writers can starve.

### Errors

`WaitTimeoutError`, `BackendUnavailableError` (fail-closed — the acquire did **not**
happen), `LockLostError`, `IncompatibleServerLogicError`, `UnsupportedError` — all
extending `RwLockError`.

### Observability

```ts
const s = await rw.inspect("order:123");
// { mode, readerCount, writerActive, queueLength, queuedWriters, oldestWaitMs, nextExpiryMs }

new RwLock(client, { metrics, tracer }); // pluggable sinks (Prometheus / OTel / StatsD)
```

## How it works (60 seconds)

Every state transition — *can this caller acquire now; who's next when a holder
releases; clean up anything expired* — runs inside a **single atomic Redis script**,
so there are no client-side read-modify-write races. Waiters block on a private
`BLPOP` mailbox and the releaser *pushes* the grant directly into it (no polling, no
pub/sub fan-out). State is **derived from source-of-truth keys** (a `readers` ZSET + a
TTL'd `writer` key), so it can't drift; a crashed writer's key self-expires and frees
the lock natively. Full design in [`SPEC.md`](https://github.com/mmdalix/redis-rwlock/blob/main/SPEC.md).

## Requirements

- **Node.js ≥ 22**
- **Redis ≥ 7** for the FUNCTION delivery path (older servers use the `EVALSHA`
  fallback automatically). Works against standalone, Sentinel, and Cluster.

## License

MIT — see [LICENSE](./LICENSE). Design spec, protocol, and contributing guide in the
[repository](https://github.com/mmdalix/redis-rwlock).
