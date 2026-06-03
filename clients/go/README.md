# redis-rwlock (Go)

[![Go Reference](https://pkg.go.dev/badge/github.com/mmdalix/redis-rwlock/clients/go.svg)](https://pkg.go.dev/github.com/mmdalix/redis-rwlock/clients/go)
[![Go CI](https://github.com/mmdalix/redis-rwlock/actions/workflows/go.yml/badge.svg)](https://github.com/mmdalix/redis-rwlock/actions/workflows/go.yml)
[![license: MIT](https://img.shields.io/badge/license-MIT-green.svg)](../../LICENSE)

> A distributed **read/write lock** over Redis for Go — many readers or one writer —
> with FIFO fair queueing (no polling), instant hand-off, leases, a watchdog, crash
> recovery with **zero background infrastructure**, and a **fencing token on every
> acquire**.

This is the Go client for [`redis-rwlock`](../../README.md). All lock logic lives in
atomic, server-side Redis scripts (a Redis FUNCTION library, with an `EVALSHA`
fallback) — **one brain, many clients**. This package is a thin, idiomatic wrapper over
[`go-redis/v9`](https://github.com/redis/go-redis). Because the semantics live in one
shared place, a Go writer and a Node reader contend on the same resource and behave
*identically* — proven by the cross-language interop test in CI.

## Install

```bash
go get github.com/mmdalix/redis-rwlock/clients/go@latest
```

```go
import rwlock "github.com/mmdalix/redis-rwlock/clients/go"
```

Requires Go 1.24+ and a reachable Redis (standalone, Sentinel, or Cluster).

## Quick start

The scoped API is the front door: it guarantees release and cancels the function's
`context.Context` the instant the lock is lost.

```go
package main

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	rwlock "github.com/mmdalix/redis-rwlock/clients/go"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	rw := rwlock.New(rdb)
	defer rw.Close()

	err := rw.WithWriteLock(context.Background(), "order:123",
		func(ctx context.Context, h *rwlock.Handle) error {
			// ctx is cancelled if the lock is lost; enforce h.FencingToken() at storage.
			return storage.Write(ctx, payload, h.FencingToken())
		},
		rwlock.Lease(30*time.Second), rwlock.Watchdog(),
	)
	if err != nil {
		// errors.Is(err, rwlock.ErrWaitTimeout) etc.
	}
}
```

Returning a value? Use the generic package functions (Go methods can't take type
parameters):

```go
total, err := rwlock.DoRead(ctx, rw, "ledger:42",
	func(ctx context.Context, h *rwlock.Handle) (int64, error) {
		return readBalance(ctx)
	})
```

## The context mapping: lock loss → cancellation

The Node client exposes an `AbortSignal` that fires on lock loss; the Go-native
equivalent is a **`context.Context` cancelled on lock loss**. The scoped API derives a
child context, cancels it (cause `ErrLockLost`) when the watchdog observes loss, and
passes it to your function — so cancellation is plumbed exactly like every other Go API.
On the raw API, reach it via `h.Context()`.

## Raw API

When you need to manage the handle yourself:

```go
h, err := rw.AcquireWrite(ctx, "order:123",
	rwlock.Lease(30*time.Second), rwlock.Wait(10*time.Second), rwlock.Watchdog())
if err != nil { /* errors.Is(err, rwlock.ErrWaitTimeout) ... */ }
defer h.Release(context.Background())

select {
case <-h.Context().Done():   // lock lost — stop touching the resource
	return context.Cause(h.Context()) // == rwlock.ErrLockLost
default:
}

token := h.FencingToken()    // enforce this at your storage layer
_ = h.Extend(ctx, 30*time.Second) // never shortens; returns ErrLockLost if gone
```

Read locks are the same with `AcquireRead` / `WithReadLock` — many readers share, a
writer excludes them.

## Errors

Sentinels, `errors.Is`-friendly; backend failures wrap the underlying go-redis error
with `%w`:

| Error | Meaning |
|---|---|
| `ErrWaitTimeout` | Couldn't acquire within the wait budget (often *not* an error — someone else holds it). |
| `ErrLockLost` | A held lease expired or was taken. Stop touching the resource. |
| `ErrBackendUnavailable` | Redis unreachable / command failed. **Fail-closed** — the acquire did *not* happen. |
| `ErrNotHeld` | Release/extend on a token not currently held. |
| `ErrIncompatibleServerLogic` | Installed module's protocol version is incompatible (SPEC §16). |
| `ErrUnsupported` | A required capability is unavailable with no acceptable fallback. |
| `ErrClosed` | The `RwLock` was closed. |

## Options

**Constructor** (`rwlock.New(client, opts...)`):

```go
rwlock.New(rdb,
	rwlock.WithDefaultLease(30*time.Second),
	rwlock.WithDefaultWait(10*time.Second),
	rwlock.WithDefaultFairness(rwlock.WritePreferring),
	rwlock.WithBlockingPoolSize(16),       // dedicated conns for BLPOP waits
	rwlock.WithKeyspaceEvents(true),       // auto-detected; never calls CONFIG SET
	rwlock.WithOnRecovery(func(res string) { /* sweep observed */ }),
	rwlock.WithLogger(slog.Default()),     // default: discard
)
```

**Per-acquire**:

```go
rw.AcquireWrite(ctx, "r",
	rwlock.Lease(30*time.Second), rwlock.Wait(10*time.Second),
	rwlock.Policy(rwlock.WritePreferring),  // or FIFO / ReadPreferring
	rwlock.Owner("worker-1"),               // defaults to "<hostname>#<pid>"
	rwlock.MaxReaderBatch(1000),
	rwlock.Watchdog(),                      // auto-extend at ~lease/3 while held
)
```

All durations are `time.Duration`, converted to integer milliseconds for the Lua. Limits
(lease, wait, reader batch) are clamped server-side (SPEC §19).

## Observability

```go
st, err := rw.Inspect(ctx, "order:123") // rwlock.Status
// st.Mode, st.ReaderCount, st.WriterActive, st.QueueLength,
// st.QueuedWriters, st.OldestWait, st.NextExpiry
```

`WithLogger` takes a `*slog.Logger`. (Metrics/Tracer interfaces — present in the Node
client — are planned for Go.)

## Is it safe?

Like **every** Redis-based distributed lock — including Redlock — this is a *lease* (a
lock with a TTL), not a linearizable lock: a holder that pauses past its lease can be
superseded. That's [inherent to distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html).
The fix is the **fencing token**: every acquire returns one (`h.FencingToken()`). Use it
for *efficiency* as-is; for *correctness* (a double-grant corrupts data) enforce it at
your storage layer — reject any write with a `≤` token. Full treatment in
[`SPEC.md`](../../SPEC.md) §2 and §12.

## How it works

Every transition runs inside a single atomic Redis script, so there are no client-side
read-modify-write races. Waiters block on a private `BLPOP` mailbox and the releaser
pushes the grant straight into it (no polling). State is derived from source-of-truth
keys, so it can't drift, and a crashed writer's key self-expires natively. The Go client
vendors a generated copy of the canonical Lua into `internal/lua` (`//go:embed`); CI's
`TestVendoredLuaInSync` guards against drift. See [`SPEC.md`](../../SPEC.md).

## Compatibility

- **Go:** 1.24+ (CI matrix: 1.24, 1.25).
- **Redis client:** [`go-redis/v9`](https://github.com/redis/go-redis) — accepts any
  `redis.UniversalClient` (`*redis.Client`, `*redis.ClusterClient`, `*redis.Ring`).
- **Protocol:** `PROTOCOL_VERSION 1`, shared with the Node client.
- **Module path / tags:** monorepo module `github.com/mmdalix/redis-rwlock/clients/go`;
  releases are tag-prefixed (`clients/go/v0.1.0`).

## Testing locally

```bash
cd clients/go
go test -race ./...   # spawns a throwaway redis-server per test (needs redis-server on PATH)
```

## License

[MIT](../../LICENSE)
