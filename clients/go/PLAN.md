# Go client — implementation plan

Status: **planned** (not started). Reference client: Node.js (`clients/node`, published as
`redis-rwlock`). This document plans the Go port over the **same shared Lua**
(`protocol/lua/`), `PROTOCOL_VERSION 1`.

## Locked decisions

- **Redis client: [`go-redis/v9`](https://github.com/redis/go-redis)** — the standard,
  most-used client; its pool model (a blocking command holds one pooled conn) fits our
  per-waiter BLPOP mailbox directly, and `redis.NewScript`/`FCall` cover delivery. We
  accept `redis.UniversalClient`, so `*redis.Client`, `*redis.ClusterClient`, and
  `*redis.Ring` all work. (rueidis is faster but its `DedicatedClient` blocking model
  fights our design; revisit behind an interface only if requested.)
- **Module path: `github.com/mmdalix/redis-rwlock/clients/go`** (monorepo). Release tags
  are **prefixed**: `clients/go/v0.1.0`. For a future v2+, the module path must end in
  `/v2` and tags become `clients/go/v2.0.0`.
- **Go version:** `go 1.24` in `go.mod` (use 1.25 features like `testing/synctest` under
  build/test only). Tool deps via `go tool` (golangci-lint, staticcheck) in `go.mod`.

## Guiding principle

The Lua in `protocol/lua/` is the brain and the single source of truth. The Go client is a
thin, idiomatic wrapper. Behaviour that decides *who gets the lock* lives in Lua and is
shared; only ergonomics/IO plumbing is per-language.

## The key mapping: AbortSignal → context.Context

The Node client exposes `lock.signal` (an `AbortSignal` that fires when the lock is lost).
The Go-native equivalent is a **`context.Context` cancelled on lock loss**. The scoped API
derives a child context, cancels it when the watchdog observes `ErrLockLost`, and passes it
to the user's function — so cancellation is plumbed exactly like every other Go API.

```go
err := rw.WithWriteLock(ctx, "order:123", func(ctx context.Context, h *rwlock.Handle) error {
    if err := doWork(ctx); err != nil { return err }            // ctx cancels on lock loss
    return storage.Write(ctx, payload, h.FencingToken())        // enforce fencing
}, rwlock.Lease(30*time.Second), rwlock.Watchdog())
// acquired, auto-extended while held, released on return (even on error)
```

## Public API

```go
package rwlock

func New(client redis.UniversalClient, opts ...Option) *RwLock

// Scoped (the documented front door) — guaranteed release + ctx cancel on loss.
func (l *RwLock) WithWriteLock(ctx context.Context, resource string,
    fn func(context.Context, *Handle) error, opts ...AcquireOption) error
func (l *RwLock) WithReadLock(ctx context.Context, resource string,
    fn func(context.Context, *Handle) error, opts ...AcquireOption) error

// Generic value-returning variants (Go methods can't take type params, so these are
// package functions): DoWrite[T] / DoRead[T].
func DoWrite[T any](ctx context.Context, l *RwLock, resource string,
    fn func(context.Context, *Handle) (T, error), opts ...AcquireOption) (T, error)

// Raw.
func (l *RwLock) AcquireWrite(ctx context.Context, resource string, opts ...AcquireOption) (*Handle, error)
func (l *RwLock) AcquireRead(ctx context.Context, resource string, opts ...AcquireOption) (*Handle, error)
func (h *Handle) Release(ctx context.Context) error
func (h *Handle) Extend(ctx context.Context, lease time.Duration) error // never shortens; ErrLockLost
func (h *Handle) Context() context.Context  // cancelled on loss (== Node's signal)
func (h *Handle) FencingToken() int64

func (l *RwLock) Inspect(ctx context.Context, resource string) (Status, error)
func (l *RwLock) Close() error

type Handle struct {
    Resource   string
    Mode       Mode      // Read | Write
    Token      string
    Fencing    int64
    LeaseUntil time.Time
    // unexported: owner *RwLock, ctx, cancel, watchdog stop, settle-once guard
}

type Status struct {
    Mode          Mode
    ReaderCount   int
    WriterActive  bool
    QueueLength   int
    QueuedWriters int
    OldestWait    time.Duration // -1 => none (use a sentinel or *time.Duration)
    NextExpiry    time.Duration
}
```

### Errors (sentinels, `errors.Is`-friendly)

```go
var (
    ErrWaitTimeout            = errors.New("rwlock: wait timeout")
    ErrBackendUnavailable     = errors.New("rwlock: backend unavailable") // fail-closed: acquire did NOT happen
    ErrLockLost               = errors.New("rwlock: lock lost")
    ErrNotHeld                = errors.New("rwlock: not held")
    ErrIncompatibleServerLogic = errors.New("rwlock: incompatible protocol version")
    ErrUnsupported            = errors.New("rwlock: unsupported capability")
)
```
Wrap backend errors with `%w`. Callers use `errors.Is(err, rwlock.ErrWaitTimeout)`.

### Options (functional)

```go
// Constructor:
rwlock.New(client,
    rwlock.WithDefaultLease(30*time.Second),
    rwlock.WithBlockingPoolSize(16),
    rwlock.WithKeyspaceEvents(true),         // "auto" detect
    rwlock.WithMetrics(m), rwlock.WithTracer(t),
    rwlock.WithLogger(slog.Default()),       // default: slog.DiscardHandler
)
// Per-acquire:
rw.AcquireWrite(ctx, "r",
    rwlock.Lease(30*time.Second), rwlock.Wait(10*time.Second),
    rwlock.Fairness(rwlock.WritePreferring), rwlock.Watchdog(),
    rwlock.Owner("worker-1"),                // defaults to "<hostname>#<pid>"
    rwlock.MaxReaderBatch(1000),
)
```
All durations are `time.Duration`; converted to integer milliseconds when calling the Lua.

## Internals (mirror the Node client)

- **Delivery** (`delivery.go`): `FunctionDelivery` via `client.FCall(ctx, name, keys, args...)`
  and `ScriptDelivery` via `redis.NewScript(src)` (which does `EVALSHA`→`EVAL` on NOSCRIPT).
  Capability probe attempts `FUNCTION LOAD REPLACE`; falls back to scripts silently.
- **Install + handshake** (`install.go`): read `rwlock:__module__`; raise
  `ErrIncompatibleServerLogic` on a different `protocol_version`; write the marker if
  absent; `loaded_at_ms` from Redis `TIME`.
- **Blocking pool** (`pool.go`): a dedicated `*redis.Client` cloned from
  `client.Options()` with `PoolSize = blockingPoolSize`, used only for `BLPOP`, so waits
  never starve the user's pool. Closed by `RwLock.Close()`.
- **Handle + watchdog** (`handle.go`): a goroutine `time.Ticker` at ~lease/3 calls
  `Extend`; on `ErrLockLost` (or the client-side safety-margin) it cancels the handle's
  context. `Release`/loss stop the ticker (no goroutine leaks); a settle-once guard fires
  release/loss metrics exactly once.
- **Wait loop**: `BLPOP` on the private mailbox via the blocking pool, bounded by the
  `next_wake` the scripts return (soonest holder lease or head request deadline); on a
  wake without a grant, run `expire_and_grant` and refresh `next_wake`.
- **Keyspace subscriber** (`keyspace.go`): optional `PSubscribe("__keyevent@*__:expired")`
  reacting to the `writer` key's native TTL; auto-detected via `CONFIG GET`, never
  `CONFIG SET`.
- **Observability**: `Metrics` and `Tracer` are small interfaces (adapt Prometheus /
  OpenTelemetry); default no-ops. `Inspect` returns `Status`.

## Lua embedding & single-source discipline

Go cannot `//go:embed` files outside its module, so the canonical Lua must be **vendored
into the module**:

```
tools/sync-lua/         # copies protocol/lua/*.lua -> clients/<lang>/<vendored dir>,
                        # prepends a "GENERATED — DO NOT EDIT" header (Node's gen:lua does
                        # the equivalent into src/lua.generated.ts)
clients/go/internal/lua/  # vendored .lua, //go:embed *.lua ; an init builds the EVALSHA
                          # bodies (lib + each script) and the FUNCTION library string.
```
CI runs **`check-sync`** (hash compare) so a hand-edited or stale vendored copy fails the
build — the same guarantee Node's `gen:lua` + drift check provides.

## Module layout

```
clients/go/
  go.mod  go.sum
  rwlock.go      # RwLock, New, AcquireWrite/Read, WithWriteLock/WithReadLock, DoWrite/DoRead
  options.go     # Option, AcquireOption (functional options) + resolved config, defaults
  errors.go      # sentinel errors
  delivery.go    # FunctionDelivery / ScriptDelivery + capability probe
  install.go     # handshake + module marker
  pool.go        # dedicated blocking client
  handle.go      # Handle, watchdog goroutine, context-cancel-on-loss, settle-once
  keyspace.go    # optional expiry subscriber
  inspect.go     # Inspect + Status
  internal/lua/  # vendored Lua (//go:embed), DO NOT EDIT
  internal/redistest/  # spawn a throwaway redis-server (mirrors the Node harness)
  rwlock_test.go conformance_test.go ...
```

## Testing

- **Integration:** spawn `redis-server` per test via `os/exec` (no Docker dependency;
  mirrors the Node harness). A `redistest` helper returns a connected client + cleanup.
- **Deterministic time tests:** use **`testing/synctest`** (Go 1.25) for the watchdog,
  self-wake, and timeout logic — fake clock, no real sleeps, no flakes.
- **Conformance:** a Go runner reads `protocol/conformance/scenarios/*.json` and asserts
  the same outcomes Node asserts.
- **Race detector** (`go test -race`) on the concurrent paths.
- **The interop capstone:** a CI job with one Redis where a **Go writer contends with a
  Node reader** (and vice versa), asserting they serialize correctly — the test that
  *proves* "one brain, many clients".

## CI/CD

- `.github/workflows/go.yml`: matrix Go 1.24/1.25; `go vet`, `staticcheck`/golangci-lint,
  `go test -race`, real Redis (apt). Path-filtered to `clients/go/**` + `protocol/**`.
- `conformance.yml`: cross-language interop (Go ↔ Node) on a shared Redis.
- Release: Go modules need no publish step — a `clients/go/v*` tag *is* the release; add a
  verify job (`go vet`, tests, `go mod verify`) gated on the tag.

## Milestones

| ID | Scope |
|----|-------|
| **G0** | Module + embedded Lua + delivery/handshake; raw `AcquireWrite`/`AcquireRead`/`Release`/`Extend`; blocking-pool BLPOP wait loop with `next_wake`; fencing; fail-closed; `errors.Is` taxonomy. **✅ done** |
| **G1** | Read locks + fairness policies (write_preferring blocks/serves; read_preferring jumps) — exercised against the shared Lua. **✅ done** |
| **G2** | `Extend` safety margin + watchdog goroutine + scoped `WithWriteLock`/`WithReadLock` (Context cancelled on loss, cause `ErrLockLost`) + generic `DoWrite`/`DoRead`. **✅ done** |
| **G3** | Recovery (self-wake + optional keyspace subscriber) + `Inspect` + metrics/tracing interfaces + slog logger. |
| **G4** | Conformance suite green + **mixed-language interop (Go↔Node)** + `sync-lua`/`check-sync` + `go.yml` CI + first `clients/go/v0.1.0` tag. |

## Open items to confirm before/during G0

- Go's `OldestWait`/`NextExpiry` "none" sentinel (`-1`): use `time.Duration(-1)` or
  `(found bool)`? Lean to returning `time.Duration` with `-1` documented (matches `inspect`).
- Whether to ship a tiny `redis-rwlock-go` subtree mirror later for a short import path
  (deferred; `clients/go` is fine for v0).
- Reentrancy stays out of v1 (matches Node + `sync.RWMutex`).
