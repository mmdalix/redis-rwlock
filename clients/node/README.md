# redis-rwlock

A distributed **read/write lock** over Redis for Node.js. Many readers may hold a
resource together, or exactly one writer holds it exclusively — with **FIFO,
queue-based waiting (no polling)**, immediate hand-off on release, per-acquisition
**leases**, **fencing tokens**, a watchdog, and crash recovery with **zero required
background infrastructure**.

All lock logic lives in atomic, server-side Redis scripts (delivered as a Redis
**FUNCTION** library, falling back to `EVALSHA`); the client is a thin wrapper over
your existing [`node-redis`](https://github.com/redis/node-redis) client.

> ⚠️ **Correctness note.** This is a **lease-based** lock. For *efficiency* (avoid
> duplicate work, reduce contention) it's safe as-is. For *correctness* (a
> double-grant would corrupt data or money) you MUST enforce the returned
> **fencing token** at your storage/service layer — the lock alone is not a
> sufficient correctness boundary. If you need a linearizable lock without fencing,
> use a consensus system (etcd/ZooKeeper/Consul).

## Install

```bash
npm install redis-rwlock redis
```

`redis` (node-redis v5 or v6) is a peer dependency — bring your own connected client.

## Usage

The **scoped API** is the front door: it guarantees release, exposes an
`AbortSignal` that fires the instant the lock is lost, and (with `watchdog`)
auto-extends long operations.

```ts
import { createClient } from "redis";
import { RwLock } from "redis-rwlock";

const client = await createClient().connect();
const rw = new RwLock(client);

await rw.withWriteLock(
  "order:123",
  { ownerId: "worker-1", leaseMs: 30_000, watchdog: true },
  async (lock) => {
    // lock.signal aborts the moment the lease is lost — pass it to your work
    await doWork({ signal: lock.signal });
    await storage.write(payload, { fencingToken: lock.fencingToken }); // enforce fencing!
  },
);
// released automatically, even on throw

await rw.withReadLock("order:123", { ownerId: "reader-1" }, async (lock) => {
  return read(lock.signal);
});
```

Raw acquire/release for power users, including `await using` for automatic release:

```ts
await using lock = await rw.acquireWrite("order:123", { ownerId: "worker-1" });
// ... use lock.fencingToken at your storage layer ...
// released automatically at end of scope
```

When you're done with the `RwLock` instance, call `await rw.close()` to release its
internal blocking-connection pool (it never touches your client).

## Options (per acquire)

| Option | Default | Notes |
|---|---|---|
| `leaseMs` | 30000 (max 300000) | how long you may hold it |
| `waitMs` | 10000 (max 60000) | how long to block waiting |
| `ownerId` | — (required) | who holds it (process/worker id) |
| `fairness` | `write_preferring` | `write_preferring` \| `fifo` \| `read_preferring` |
| `watchdog` | `false` | auto-extend at ~lease/3 while held |
| `signal` | — | `AbortSignal` to cancel a pending acquire |
| `maxReaderBatch` | 1000 | cap readers woken by one grant |

`new RwLock(client, config)` also accepts `metrics` / `tracer` sinks, a
`blockingPoolSize`, `keyspaceEvents`, and more — see the types.

## Errors

`WaitTimeoutError`, `BackendUnavailableError` (fail-closed — the acquire did **not**
happen), `LockLostError`, `IncompatibleServerLogicError`, `UnsupportedError`, all
extending `RwLockError`.

## Observability

```ts
const status = await rw.inspect("order:123");
// { mode, readerCount, writerActive, queueLength, queuedWriters, oldestWaitMs, nextExpiryMs }
```

Plus pluggable `metrics`/`tracer` sinks (Prometheus / OpenTelemetry / StatsD).

## Requirements

- Node.js ≥ 22
- Redis ≥ 7 for the FUNCTION delivery path (older servers use the `EVALSHA`
  fallback automatically). Works against standalone, Sentinel, and Cluster.

## License

MIT — see [LICENSE](./LICENSE). Full design spec and protocol in the
[repository](https://github.com/mmdalix/redis-rwlock).
