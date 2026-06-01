# redis-rwlock

A distributed **read/write lock** over Redis. Many readers may hold a resource
together, or exactly one writer holds it exclusively — with FIFO, queue-based
waiting (no polling), immediate hand-off on release, per-acquisition leases,
fencing tokens, and **zero required background infrastructure**.

All lock logic lives in atomic, server-side Redis scripts; clients are thin
wrappers. This guarantees identical semantics across language ports. The full
design is in [`SPEC.md`](./SPEC.md); the build sequence is in [`PLAN.md`](./PLAN.md).

> ⚠️ **Correctness note (read this).** This library provides **lease-based** locks.
> For *efficiency* (don't do duplicate work, reduce contention) it is safe as-is.
> For *correctness* (a double-grant would corrupt data or money), you MUST enforce
> the returned **fencing token** at your storage/service layer — the lock alone is
> not a sufficient correctness boundary. See `SPEC.md` §2 and §12.

## Status

Early development. The shared Lua protocol (`protocol/lua/`) and the **Node.js**
reference client (`clients/node/`) implement the M0 slice: write locks, queueing,
hand-off, fencing, crash recovery via self-wake, and `extend`. See `PLAN.md` for the
milestone roadmap and exactly what is and isn't implemented yet.

## Repository layout

- `protocol/` — language-agnostic source of truth: the Lua scripts and the
  cross-language conformance scenarios.
- `clients/node/` — the Node.js/TypeScript client (wraps `ioredis`).

## Node usage (preview)

```ts
import Redis from "ioredis";
import { RwLock } from "@org/redis-rwlock";

const rw = new RwLock(new Redis());

const h = await rw.acquireWrite("order:123", { ownerId: "worker-1", leaseMs: 30_000 });
try {
  // ... do work; enforce h.fencingToken at your storage layer ...
} finally {
  await rw.release(h);
}
```

The scoped/closure API (`withWriteLock`, automatic release, watchdog, cancellation)
is the documented front door and arrives in milestone M3.

## Development

```bash
cd clients/node
npm install
npm run gen:lua   # embed protocol/lua -> src/lua.generated.ts
npm test          # spins up a local redis-server per test file
```

Requires a `redis-server` binary on PATH for the test suite.
