# redis-rwlock

[![npm version](https://img.shields.io/npm/v/redis-rwlock.svg)](https://www.npmjs.com/package/redis-rwlock)
[![CI](https://github.com/mmdalix/redis-rwlock/actions/workflows/ci.yml/badge.svg)](https://github.com/mmdalix/redis-rwlock/actions/workflows/ci.yml)
[![license: MIT](https://img.shields.io/badge/license-MIT-green.svg)](./LICENSE)

> A distributed **read/write lock** over Redis — many readers or one writer — with
> FIFO fair queueing (no polling), instant hand-off, leases, a watchdog, crash
> recovery with **zero background infrastructure**, and a **fencing token on every
> acquire**. Honest about correctness, ergonomic to use.

The organizing idea: **one brain, many clients.** All lock logic lives in atomic,
server-side Redis scripts (a Redis FUNCTION library, with an `EVALSHA` fallback);
each language client is a thin wrapper over the user's existing Redis client. That's
what lets a Go writer and a Python reader contend on the same resource and behave
*identically* — the semantics live in one place, not reimplemented per language.

## Is it safe?

Like **every** Redis-based distributed lock — **including Redlock** — this is a *lease*
(a lock with a TTL), not a linearizable lock: a holder that pauses past its lease can be
superseded. That's [inherent to distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html),
not a quirk of this project. The fix is a **fencing token** — and unlike plain Redlock,
**every acquire here returns one** (`fencingToken`). Use it as-is for *efficiency*
(dedupe work, reduce contention); for *correctness* (a double-grant corrupts data or
money) enforce that token at your storage layer (reject any write with a `≤` token).
Full treatment in [`SPEC.md`](./SPEC.md) §2 and §12.

## Highlights

- 🔁 **Read/write semantics** — shared readers or one exclusive writer.
- 🎟️ **Fencing tokens** on every grant — the real correctness boundary.
- 🚦 **Fair, poll-free queueing** — waiters are *handed* the lock via a private mailbox;
  `write_preferring` / `fifo` / `read_preferring` policies; no retry storms, no
  cluster-wide pub/sub fan-out.
- ⏱️ **Leases + opt-in watchdog**, 🧯 **crash recovery with no dispatcher and no
  `CONFIG SET`**, 🧩 **scoped API** (guaranteed release, `AbortSignal`, `await using`).
- 🧠 **Atomic Lua state machine** delivered as a Redis FUNCTION library (or `EVALSHA`),
  with a cross-version handshake, capability detection, and standalone/Sentinel/Cluster
  support.
- 📈 **Observability** — `inspect()`, pluggable metrics/tracing.

## Quickstart (Node.js)

```bash
npm install redis-rwlock redis
```

```ts
import { createClient } from "redis";
import { RwLock } from "redis-rwlock";

const rw = new RwLock(await createClient().connect());

await rw.withWriteLock("order:123", { leaseMs: 30_000, watchdog: true }, async (lock) => {
  await doWork({ signal: lock.signal });                         // signal aborts if the lock is lost
  await storage.write(payload, { fencingToken: lock.fencingToken }); // enforce fencing
});
```

Full Node docs and API: **[`clients/node`](./clients/node) / [npm](https://www.npmjs.com/package/redis-rwlock)**.

## Packages

| Language | Package | Status |
|---|---|---|
| Node.js / TypeScript | [`redis-rwlock`](https://www.npmjs.com/package/redis-rwlock) (`clients/node`) | ✅ published |
| Go | — | 🛣️ planned (same shared Lua) |
| Python | — | 🛣️ planned |

The Lua protocol and conformance scenarios in [`protocol/`](./protocol) are the shared
source of truth every client port runs against.

## Repository layout

```
SPEC.md            # the design contract (single source of truth)
COMPATIBILITY.md   # protocol versions & client ⇄ protocol mapping
PLAN.md            # milestone roadmap
protocol/
  VERSION          # PROTOCOL_VERSION
  lua/             # the shared, atomic Lua scripts (the "brain")
  conformance/     # language-agnostic scenarios every client must pass
clients/
  node/            # the Node.js / TypeScript client
```

## How it works

Every transition — *can this caller acquire now; who is next when a holder releases;
clean up anything expired* — runs inside a **single atomic Redis script**, so there are
no client-side read-modify-write races. Waiters block on a private `BLPOP` mailbox and
the releaser pushes the grant straight into it (no polling). State is **derived from
source-of-truth keys** (a `readers` ZSET + a TTL'd `writer` key), so it can't drift, and
a crashed writer's key self-expires to free the lock natively. Leases are the
deadlock backstop; fencing tokens are the correctness backstop. See [`SPEC.md`](./SPEC.md).

## Documentation

- **[SPEC.md](./SPEC.md)** — the full protocol & design contract (key schema, scripts,
  correctness model, Appendix A pseudocode).
- **[COMPATIBILITY.md](./COMPATIBILITY.md)** — protocol versioning and compatibility.
- **[PLAN.md](./PLAN.md)** — milestone roadmap.

## Contributing

The Lua in `protocol/lua/` is the single source of truth — clients vendor a generated
copy (`npm run gen:lua`) and must never hand-edit it; CI guards against drift. To work on
the Node client:

```bash
cd clients/node
npm install
npm test        # spins up a throwaway redis-server per test file (needs redis-server on PATH)
```

## License

[MIT](./LICENSE)
