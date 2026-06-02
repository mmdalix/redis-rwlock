# Distributed Read/Write Lock over Redis — Implementation Specification

**Status:** Ready for implementation
**Version:** Spec v1.0 · targets wire/protocol `PROTOCOL_VERSION = 1`
**Audience:** Engineers (or coding agents) building the library in Node.js, Python, Go, Java, etc.
**Deliverable being specified:** a *client library*, published to npm / PyPI / Go modules / Maven, that talks to a Redis the **user already runs**. No server, daemon, or sidecar that we operate. It must work the moment a user installs it and points it at their Redis.

---

## 0. How to read this document

This spec is the single source of truth. It is written so that a team can implement the library in any language and have the implementations be **mutually compatible** — a Go writer and a Python reader contending on the same resource must behave identically.

The design has one organizing principle:

> **All lock logic lives in atomic, server-side Redis scripts. Clients are thin wrappers.**

Everything in this document is either (a) the server-side contract (key schema + scripts + payload formats — Sections 4–9), which is *identical across all languages and must not be reimplemented per language*, or (b) the client responsibilities (Sections 10–17), which are *idiomatic per language but must obey the same contract*.

If you implement only one thing first, implement Sections 4–9 exactly, and verify them with the conformance suite in Section 20. Everything else is a wrapper around that.

**Table of contents**

1. Goals and non-goals
2. Correctness model (read this before writing any code)
3. Architecture principles
4. Redis data model (key schema)
5. Time, IDs, and tokens
6. Server-side scripts — overview and the shared grant algorithm
7. Script contracts (inputs / outputs)
8. Acquisition protocol (client side)
9. Release, extension, and the watchdog
10. Auto-release and crash recovery (library mode — no dispatcher)
11. Fairness policies and writer-starvation prevention
12. Fencing tokens (the real correctness boundary)
13. Public API contract + idiomatic examples
14. Error taxonomy
15. Connection management
16. Versioning and cross-version compatibility
17. Capability detection and graceful degradation
18. Observability
19. Configuration and defaults
20. Testing and acceptance criteria
21. Build plan / milestones
22. Per-language porting guide
- Appendix A: full script pseudocode
- Appendix B: payload and handle formats
- Appendix C: glossary

---

## 1. Goals and non-goals

### 1.1 Goals

- A **distributed `RWMutex`**: many readers may hold a resource together, or exactly one writer holds it exclusively.
- **Queue-based waiting, FIFO**, with **no polling and no retry storms**. A waiter blocks until it is *directly handed* the lock.
- **Immediate handoff**: releasing a lock wakes the next eligible waiter(s) in the same atomic step that frees the lock.
- **Per-acquisition timeouts**: caller chooses lease duration (how long it may hold) and wait timeout (how long it will block) on every call.
- **Lock extension** while holding, plus an optional **watchdog** that auto-extends long operations.
- **Configurable writer priority** to prevent writer starvation; default is write-preferring (matches Go's `sync.RWMutex`).
- **Auto-release**: leases expire so a crashed holder never deadlocks the resource — with **zero required background infrastructure**.
- **Language independence by construction**: the same server-side logic is shared by all clients.
- **Fencing tokens** on every acquisition for correctness-sensitive use.
- Works against **standalone, Sentinel, and Cluster** Redis, including **managed Redis** (ElastiCache, Upstash, Memorystore, Azure Cache) where some admin commands are restricted.

### 1.2 Non-goals (explicitly out of scope for v1)

- Semaphores, leader election, rate limiters, distributed counters, barriers. (A plain mutex is supported as the trivial special case: a write lock with the read path unused.)
- A managed service, broker, or required background worker. Recovery must not depend on us operating anything.
- A pluggable backend abstraction (etcd / Postgres / ZooKeeper). Generalizing the backend forces lowest-common-denominator semantics and breaks direct handoff and single-source-of-truth fencing. **Be the best Redis RW lock, not a mediocre everything-lock.**
- Reentrancy is **off by default** (matches Go's non-reentrant `sync.RWMutex`). It is an optional extension (Section 9.4); v1 cores need not ship it.

---

## 2. Correctness model — read this before writing any code

This is the most important section. A lock library that overpromises is worse than useless, because callers will trust it.

### 2.1 A Redis lock is a *lease*, not a perfect mutual-exclusion barrier

A lock holder can pause (GC, OS scheduling, VM migration, network stall) for longer than its lease. The lease then expires, Redis grants the lock to someone else, and the original holder *resumes believing it still holds the lock*. Now two writers act at once. **No Redis-side cleverness removes this**, because the unsafe gap is between the client and the resource it is protecting, not inside Redis. Likewise, asynchronous primary→replica failover can lose a just-granted lock if the primary dies before replicating it.

Therefore the library MUST:

1. **Issue a fencing token on every successful acquisition** (Section 12) and make it trivial to enforce.
2. **Fail closed.** If Redis is unreachable or a command errors, acquisition returns an error. It never silently succeeds.
3. **Cancel the caller's work when the lock is lost.** The scoped/closure API (Section 13) ties an `AbortSignal` / `context.Context` / cancellation token to lock liveness, so a holder whose lease lapses has its in-flight work cancelled rather than continuing blindly.

### 2.2 What is guaranteed, and when

**While the Redis primary is available and reachable:**
- At most one writer holds a resource at any time.
- Readers hold only when no writer holds.
- A writer holds only when no readers hold.
- Wait order is FIFO for *queued* waiters, with contiguous readers batch-granted together. (Under the default `write_preferring`, a fresh reader may be granted immediately while no writer is queued rather than queuing — strict head-of-line FIFO for new readers requires the `fifo` policy; see §11.)
- Releasing a lock wakes eligible waiters immediately.
- Waiters do not poll.

**Under client crash:**
- Held locks expire automatically (lease TTL).
- Pending wait requests expire automatically (request-key TTL).
- The next waiter is woken without anyone calling release (Section 10).

**Under Redis failover / partition:**
- Mutual exclusion is **not** guaranteed by the lock alone. Correctness in this regime comes from **fencing tokens enforced at the protected resource**. Optionally `WAIT`/`WAITAOF` after grant narrows (does not eliminate) the loss window at a latency cost.

### 2.3 Guidance to document for users

State this verbatim in the README:

> This library provides **lease-based** read/write locks. For *efficiency* (don't do duplicate work, reduce contention) it is safe and simple to use as-is. For *correctness* (a double-grant would corrupt data or money), you MUST enforce the returned fencing token at your storage/service layer; the lock alone is not a sufficient correctness boundary. If you need a linearizable lock without fencing, use a consensus system (etcd, ZooKeeper, Consul) instead.

---

## 3. Architecture principles

1. **Atomic server-side state machine.** Every state transition ("can this caller acquire now; if a holder just released, who is next; clean up anything expired") runs inside a single Redis script executed atomically. There are **no client-side read-modify-write races** because clients never compute lock decisions.

2. **Per-waiter blocking mailboxes, not shared pub/sub.** Each waiter blocks on its own private list via `BLPOP`. The grant is *pushed* into that list by whoever frees the lock. This gives: no polling, no retry storm, exactly-one-waiter wakeups, and **no cluster-wide fan-out** (a fatal scaling problem of classic pub/sub in Redis Cluster, where every published message is broadcast to every node). The pushed grant also persists in the list, so a brief client reconnect does not lose the wakeup.

3. **Leases everywhere.** Every holder entry carries an absolute expiry. Every script evicts expired holders before deciding anything. This is the deadlock-prevention backstop and requires no background process.

4. **No required infrastructure.** Recovery uses (a) lazy cleanup on every operation, (b) a bounded per-waiter self-wake tied to the current holder's lease deadline, and optionally (c) keyspace-expiry events when the user's Redis has them enabled. We never require a dispatcher, and we never require `CONFIG SET`.

5. **One brain, many clients.** The Lua logic is delivered as a Redis **Functions** library when available, falling back transparently to **`EVALSHA`** (script cache) when `FUNCTION LOAD` is restricted. Same source either way. This is what guarantees identical semantics across languages.

6. **Sit on the user's existing client.** Wrap the standard Redis client of each language (`ioredis`/`node-redis`, `redis-py`, `go-redis`, Lettuce/Jedis). Do not bundle a connection layer that fights theirs.

---

## 4. Redis data model (key schema)

All keys for one resource share the hash tag `{<resource>}` so that, in Redis Cluster, every key a script touches lives on one slot. **This is mandatory** — a Lua/Function call may only touch keys in one slot.

**Key-passing convention (mandatory, all ports):** every script/function receives **exactly one key** in `KEYS[1]` — the resource *prefix* `rwlock:{<resource>}` — and derives all other keys from it by suffix (`:readers`, `:writer`, …) inside the script. Do **not** pass the individual keys in `KEYS`; the parameters after the prefix are `ARGV` (Section 7). This is safe in Cluster because every derived key shares the `{<resource>}` hash tag. The resource name MUST NOT contain `{` or `}`.

Let `r` = the user-supplied resource name (e.g. `order:123`). The keys are:

```
rwlock:{r}:readers        ZSET    member = reader token, score = expire_at_ms   (live readers)
rwlock:{r}:writer         HASH    { token, expire_at_ms }  (the single writer; PEXPIRE'd to lease)
rwlock:{r}:queue          ZSET    member = request_id,   score = seq            (FIFO; O(log N) cancel)
rwlock:{r}:req:{id}       HASH    one per waiting request (see 4.5)             (+ TTL)
rwlock:{r}:notify:{id}    LIST    waiter's private BLPOP mailbox                (+ TTL)
rwlock:{r}:seq            STRING  INCR -> monotonic queue order
rwlock:{r}:fence          STRING  INCR -> monotonic fencing token
```

Plus one global key, written once when the script module is installed:

```
rwlock:__module__         HASH    { protocol_version, impl_version, sha, loaded_at_ms }
```

### 4.1 State is DERIVED, not cached

There is **no denormalized state cache and no per-holder metadata**. Lock state is computed O(1) directly from the source-of-truth structures, so it cannot drift:

- **mode** = `write` if `rwlock:{r}:writer` exists (and is live); else `read` if `ZCARD readers > 0`; else `none`.
- **reader_count** = `ZCARD rwlock:{r}:readers` (after evicting expired readers).
- **writer present** = `EXISTS rwlock:{r}:writer` (after clearing an expired writer).
- **queued_writers** = derived by the prune step (Appendix A) from the live `req` hashes in the queue — **never** an incremental counter. (An incremental counter cannot be maintained when a queued waiter crashes and its `req` hash TTL-expires, leaving an orphan queue entry; deriving it from truth is the only drift-free option.)

### 4.2 `rwlock:{r}:readers` — sorted set (live readers)

One entry per live reader. `score = expire_at_ms` (Redis server time). Evict expired readers with `ZREMRANGEBYSCORE readers 0 <now>`; count with `ZCARD`. Tokens are self-describing (Section 5), so no separate reader metadata is stored.

### 4.3 `rwlock:{r}:writer` — hash (the single writer)

```
token        = the writer's grant token
expire_at_ms = absolute server-time lease expiry
```

The key is `PEXPIRE`'d to the lease, so a crashed writer's key **self-expires natively** (reclaiming the lock without any library action and firing a `__keyevent@<db>__:expired` event used by the optional recovery subscriber, Section 10.3). Liveness is nonetheless re-checked against `expire_at_ms` so behavior never depends on Redis's sampled-expiry timing. A writer holds iff the key exists and `expire_at_ms > now`.

### 4.4 `rwlock:{r}:queue` — sorted set (FIFO wait queue)

`member = request_id`, `score = seq` (from `INCR rwlock:{r}:seq`). FIFO order = ascending score. A ZSET (not a list) makes cancellation a direct `ZREM` in O(log N).

### 4.5 `rwlock:{r}:req:{id}` — hash (one per waiting request)

```
mode             = "read" | "write"
owner_id         = caller-supplied owner identity
lease_ms         = requested hold duration (applied when granted)
wait_deadline_ms = absolute server-time deadline for waiting
notify_key       = rwlock:{r}:notify:{id}
granted_token    = ""  (set to the token when this request is granted)
created_at_ms    = server time at enqueue
fairness         = "write_preferring" | "fifo" | "read_preferring"
max_reader_batch = integer
```

`fairness` and `max_reader_batch` are persisted per request because **grant decisions read them from the head request** (the governing policy is the head request's — fairness is expected uniform per resource namespace). TTL: `wait_ms + request_key_ttl_grace_ms` (default grace 60s) as a GC backstop; correctness does not depend on it — the prune step (Appendix A) removes timed-out and orphaned entries and is authoritative.

### 4.6 `rwlock:{r}:notify:{id}` — list (private mailbox)

The waiter blocks here: `BLPOP rwlock:{r}:notify:{id} <seconds>`. On grant the script does `LPUSH notify_key <grant_payload>` then `PEXPIRE notify_key max(notify_key_ttl_ms, lease_ms)` — so a granted-but-undrained mailbox never expires before the holder it created (a brief reconnect cannot lose the grant). Grant payload format in Appendix B.

---

## 5. Time, IDs, and tokens

- **Time source is Redis, always.** Scripts obtain `now_ms` from the Redis `TIME` command inside the script; clients pass *durations* (`lease_ms`, `wait_ms`), never absolute timestamps. This removes cross-client clock-skew from all expiry decisions. (`TIME` is permitted in scripts on modern Redis under effects replication.)
- **`request_id`**: unique per acquire attempt. Recommended: a sortable unique id (UUIDv7 / ULID). MUST NOT contain `:`, `{`, or `}` (it is embedded in the token and used as a key suffix). Any format satisfying uniqueness + this charset interoperates.
- **`token`**: unique per *granted* hold. The canonical grammar is **`"<owner_id>:<request_id>:<fencing>"`** — minted by the shared scripts (a port must never mint tokens itself). `owner_id` MUST NOT contain `:`. The token is required to release or extend; release/extend MUST verify the token, so a caller can never release another holder's lock. Treat the token as opaque; the grammar is fixed only so all ports mint identical, collision-free tokens (`request_id` guarantees per-attempt uniqueness, `fencing` per-grant).
- **`owner_id`**: a caller-chosen identity for "who" holds it (process id, worker id, logical actor); MUST NOT contain `:`. `require_owner_id` defaults to true.
- **`fencing`**: monotonic integer from `INCR rwlock:{r}:fence`, returned on every grant (Section 12).

---

## 6. Server-side scripts — overview and the shared grant algorithm

Six scripts. All share one internal routine, `grant_from_queue`.

```
acquire            try immediate grant; else enqueue and return a mailbox to block on
release            free a holder; immediately grant the next eligible waiter(s)
extend             renew the calling holder's lease (never shortens it)
cancel_wait        remove a timed-out/cancelled waiter; reconcile a last-instant grant
expire_and_grant   maintenance: evict expired holders/requests, then grant eligible waiters
inspect            read-only debug snapshot (no-writes; Section 18)
```

The mutating scripts begin with **sweep** (evict expired readers via `ZREMRANGEBYSCORE readers 0 now`; clear an expired `writer`) and **prune** (drop timed-out/orphaned queue entries and derive `queued_writers` from the survivors). State is recomputed from truth, never cached, so correctness is self-healing on every operation.

### 6.1 `grant_from_queue(r, now)` — the heart of the system

This decides who wakes up. It is called after any event that may free capacity (a release, a cancellation that unblocks the head, or an expiry sweep).

```
grant_from_queue(r, now):
  sweep(r, now)                            # evict expired readers; clear expired writer
  prune_queue(r, now)                      # drop timed-out/orphan reqs; recompute queued_writers
  if writer present:                       # a writer holds -> nobody else proceeds
      return 0

  head = first request in queue (lowest seq)
  if head is nil:
      return 0
  fairness = head.fairness                 # the head request's policy governs

  if reader_count > 0:                      # readers hold -> only MORE readers can join
      if fairness == read_preferring:
          return grant_readers_anywhere(r, now)   # grant readers, SKIPPING queued writers
      return grant_contiguous_readers(r, now)     # stop at the first queued writer

  # no holders at all
  if fairness == read_preferring:
      g = grant_readers_anywhere(r, now)          # readers jump queued writers (writers may starve)
      if g > 0: return g
      if head.mode == "write": grant_writer(head, now); return 1
      return 0
  if head.mode == "write":
      grant_writer(head, now); return 1
  return grant_contiguous_readers(r, now)
```

`grant_contiguous_readers` walks the queue from the head, granting consecutive `read` requests and **stopping at the first `write` request** (so a queued writer is never starved past its position) and after `max_reader_batch` grants. `grant_readers_anywhere` (used only under `read_preferring`) grants **every** queued reader in FIFO order, *skipping over* queued writers — which is why `read_preferring` can starve writers, as documented. Each granted reader is added to `readers` with `score = now + lease_ms`, has `granted_token` written into its `req` hash, and gets a grant payload pushed to its `notify_key`. A granted writer sets the `writer` key (PEXPIRE'd to the lease).

> Any readers beyond the batch cap remain queued and are woken on the next transition. They are never lost.

### 6.2 Worked example (write-preferring / FIFO)

```
Initial: writer W1 holds X.
Arrivals queue as: R1 R2 W2 R3

W1 releases -> grant_from_queue:
    no holders; head R1 is a reader -> batch contiguous readers up to first writer
    grants R1, R2 ; stops at W2
    queue: W2 R3 ; R3 does NOT jump ahead of W2

R1 and R2 both release -> grant_from_queue:
    no holders; head W2 is a writer -> grant exactly W2
    queue: R3

W2 releases -> grant_from_queue:
    no holders; head R3 is a reader -> grant R3
    queue: empty
```

High read throughput, FIFO fairness, no writer starvation. Each waiter is woken only by its own mailbox.

---

## 7. Script contracts (inputs / outputs)

Every script receives `KEYS[1] = rwlock:{r}` (the prefix; Section 4) plus `ARGV`. `now_ms` is read from `TIME` inside the script — never passed in. All limit-style ARGV (`lease_ms`, `wait_ms`, `max_reader_batch`) are **clamped server-side** to the protocol limits (Section 19), so a misbehaving client cannot impose an out-of-range lease on others.

### 7.0 Decoding contract (all scripts)

Every script returns a **flat positional array** (RESP). Clients MUST decode by position and MUST NOT rely on map decoding — under RESP3 some clients surface Lua tables as maps; pin array decoding. Integers are returned as integers, strings as strings; clients coerce defensively. `inspect` returns a positional array (Section 7.6), not the JSON object shown for readability in Section 18.

### 7.1 `acquire`

`ARGV`: `mode("read"|"write")`, `lease_ms`, `wait_ms`, `request_id`, `owner_id`, `fairness`, `max_reader_batch`, `notify_key_ttl_ms`, `request_key_ttl_grace_ms`

Returns one of:

```
["GRANTED", token, fencing, lease_until_ms, mode]
["QUEUED",  request_id, notify_key, wait_deadline_ms, next_wake_ms]
```

`next_wake_ms` is the soonest absolute time at which something actionable could happen for this waiter — the soonest of any holder's lease expiry and the head queued request's `wait_deadline_ms` (or `-1` if neither). The client blocks until then to self-wake (Section 8/10); this is what lets a waiter reclaim a *crashed queued holder or head request* rather than blocking for its full `wait_ms`. A read may be granted immediately when no writer holds and (policy is read_preferring; or write_preferring and no writer queued; or fifo and the queue is empty). A write may be granted immediately when there are no holders and the queue is empty.

### 7.2 `release`

`ARGV`: `token`, `notify_key_ttl_ms`

Returns:

```
["OK"]          token was a live holder (writer or reader); removed; grant_from_queue executed
["NOT_HELD"]    token absent or already expired (idempotent no-op for the caller)
```

### 7.3 `extend`

`ARGV`: `token`, `lease_ms`

Returns:

```
["OK", new_lease_until_ms]   token is the live holder; expiry updated (never moved earlier)
["LOST"]                     token is no longer a holder (expired or released) -> caller lost the lock
```

Extend never shortens the lease (GT semantics), so passing a smaller `lease_ms` cannot drop the lock early. The "extend only within a safety margin" rule is enforced **client-side** before calling (Section 9.2); the script only checks liveness and renews.

### 7.4 `cancel_wait`

`ARGV`: `request_id`, `notify_key_ttl_ms`

Returns:

```
["CANCELLED"]            request was still queued; removed; grant_from_queue re-run if it was the head
["RECLAIMED", token]     request had already been granted at the last instant; the just-granted
                         holder was released and grant_from_queue re-run (see 8.4)
["GONE"]                 request not found; any orphan queue entry is dropped
```

`cancel_wait` MUST be called at most once per request (the client calls it exactly once, in `finish_timeout`).

### 7.5 `expire_and_grant`

`ARGV`: `notify_key_ttl_ms`

Returns:

```
["OK", granted_count, next_wake_ms]
```

Pure maintenance: sweep, prune, `grant_from_queue`. `next_wake_ms` lets a waiter refresh its self-wake boundary after running maintenance. Used by the self-wake path and (optionally) the keyspace-expiry handler; **not** required for normal releases, which grant directly.

### 7.6 `inspect` (read-only)

`ARGV`: `notify_key_ttl_ms` is **not** taken; `ARGV`: (none beyond the key). Registered as a **no-writes** function. Returns a positional array:

```
[mode("none"|"read"|"write"), reader_count, writer_active(0|1),
 queue_length, queued_writers, oldest_wait_ms(-1 if none), next_expiry_ms(-1 if none)]
```

---

## 8. Acquisition protocol (client side)

The client is thin. The full wait algorithm:

### 8.1 Acquire

```
request_id = new_sortable_id()
res = CALL acquire(r, mode, lease_ms, wait_ms, request_id, owner_id, fairness, max_reader_batch)

if res.status == "GRANTED":
    return Handle{ r, mode, token: res.token, fencing: res.fencing,
                   lease_until_ms: res.lease_until_ms }

# QUEUED -> block on the private mailbox, no polling
notify_key       = res.notify_key
wait_deadline_ms = res.wait_deadline_ms
next_wake_ms     = res.next_wake_ms
return wait_for_grant(r, request_id, notify_key, wait_deadline_ms, next_wake_ms)
```

### 8.2 `wait_for_grant` — block until handed the lock

The waiter blocks on `BLPOP`. It only ever "wakes early" at `next_wake_ms` — the soonest crashed-holder lease boundary **or** the head queued request's deadline (so a crashed *queued* head, not just a crashed holder, is reclaimed). Between boundaries it is fully blocked. This is **not polling** — it is a single bounded check tied to a concrete deadline, and it disappears entirely when keyspace events are enabled. The server-time offset is derived once from a `TIME` sample at init; `epsilon`/`floor`/`clamp` are client-local, non-interop tuning constants.

```
wait_for_grant(r, request_id, notify_key, wait_deadline_ms, next_wake_ms):
  loop:
      remaining_ms = wait_deadline_ms - server_now_estimate()
      if remaining_ms <= 0:
          return finish_timeout(r, request_id, notify_key)

      # block until the next interesting event (a lease boundary or the head's deadline)
      # or our own wait deadline, whichever is sooner
      boundary_ms = (next_wake_ms > 0) ? (next_wake_ms - server_now_estimate() + epsilon) : remaining_ms
      blpop_ms    = clamp(min(remaining_ms, boundary_ms), floor_ms, remaining_ms)

      payload = BLPOP(notify_key, blpop_ms / 1000)     # dedicated blocking connection (Section 15)
      if payload != nil:
          return Handle.from(payload)                  # GRANTED, immediate

      # woke without a grant -> a holder/head may have crashed; run maintenance once,
      # refresh the boundary from its return, then re-block
      [_, _, next_wake_ms] = CALL expire_and_grant(r)  # may push a grant into our mailbox
      # loop; we will receive any grant via the next BLPOP
```

`finish_timeout` performs the last-instant reconciliation:

```
finish_timeout(r, request_id, notify_key):
  payload = LPOP(notify_key)            # non-blocking final drain: were we granted at the buzzer?
  if payload != nil:
      return Handle.from(payload)       # take it — we got the lock just in time
  res = CALL cancel_wait(r, request_id)
  if res == ["RECLAIMED", token]:       # ultra-rare drain/cancel race; lock was reclaimed for us
      return Timeout()                  # report timeout; nothing leaked
  return Timeout()
```

### 8.3 Why this satisfies "no polling, no retry"

- The normal wakeup is a **direct push** by the releaser into this waiter's mailbox; the `BLPOP` returns immediately. No re-attempt of `acquire`.
- `expire_and_grant` is **maintenance**, not a retry: it never tries to acquire *for the caller*; it just frees expired capacity, after which the caller is granted via its mailbox like any other grant.
- The only timer is one wake per *crashed-holder lease boundary*, bounded and concrete — not a spin loop. With keyspace events on, even that is unnecessary.

### 8.4 The ghost-grant race, handled

If a waiter's wait deadline elapses at the same instant `grant_from_queue` grants it (holder created, payload pushed), the design stays correct and leaks nothing:

1. `finish_timeout` first does a non-blocking `LPOP` of its own mailbox. The common case: it finds the just-pushed grant and **takes the lock** (returns a handle).
2. Only if the drain is empty does it call `cancel_wait`. Redis serializes scripts, so either the grant already moved the request out of the queue (then `cancel_wait` finds `granted_token` set, releases that holder, re-runs `grant_from_queue`, returns `RECLAIMED`), or it was still queued (`CANCELLED`).
3. The lease is the ultimate backstop: even an abandoned grant expires and is handed on.

---

## 9. Release, extension, and the watchdog

### 9.1 Release

`CALL release(token)`. The script removes the holder, recomputes state, and immediately `grant_from_queue` (waking the next waiter(s) via their mailboxes). `NOT_HELD` is a benign no-op (the lease may have already expired). The scoped API (Section 13) calls release in `finally`/`defer` so it always runs.

### 9.2 Extension

`CALL extend(token, lease_ms)`. Returns `OK` with the new expiry, or `LOST` if the token is no longer a holder. A `LOST` result means **the lock was lost mid-operation** — the caller must stop touching the resource. The scoped API turns `LOST` into cancellation of the caller's context/signal.

**Client-side safety margin (required):** only attempt extension while `server_now < lease_until_ms - extension_margin_ms`, where `extension_margin_ms = max(500, p99_redis_latency_ms * 4)`. Extending too close to expiry risks the renewal landing after the lock was already reclaimed; within the margin the client treats the lock as lost. This margin *reduces but does not eliminate* the race — the script's own server-side `expire_at_ms <= now` re-check is the actual safety net, and `extend` never moves the expiry earlier (so a shorter `lease_ms` cannot drop the lock).

### 9.3 Watchdog (opt-in)

When enabled, a client-side timer refreshes the lease at roughly `lease_ms / 3` intervals while the holder is alive, so open-ended work does not silently drop the lock. Default **off** (predictable, fixed-lease behavior). When the watchdog observes a `LOST`, it cancels the caller's context/signal. Mutually exclusive intent with passing a fixed lease you expect to be final.

### 9.4 Reentrancy (optional extension, default off)

To support the same `owner_id` re-acquiring, maintain a per-owner refcount and have release decrement rather than remove until zero. Any added fields are a `PROTOCOL_VERSION` bump and must be specified before use. Keep it clearly flagged and off by default; v1 cores may omit it. Matches Go's non-reentrant `sync.RWMutex` when off.

---

## 10. Auto-release and crash recovery (library mode — no dispatcher)

**There is no required background process.** Recovery is layered, each layer needing strictly less from the environment than the last:

1. **Lazy cleanup (always).** Every `acquire`/`release`/`extend`/`expire_and_grant` evicts expired holders and timed-out requests before deciding anything. This alone reclaims any crashed holder the moment *any* later operation touches the resource — which fully covers the "crashed, and nobody is waiting" case for free.

2. **Per-waiter self-wake (always, zero dependencies).** A waiter knows the current holder's lease deadline (returned by `acquire` and refreshed by `expire_and_grant`). It blocks until that boundary; if no grant arrived, the holder likely crashed, so it runs `expire_and_grant` once and re-blocks. Bounded, concrete, not a poll (Section 8.2). This makes crash recovery work even on Redis where keyspace events are disabled.

3. **Keyspace-expiry events (optional, opt-in, auto-detected).** The `writer` key is `PEXPIRE`'d to its lease, so a **crashed writer's key self-expires natively** — Redis reclaims it without any library action, and (if `notify-keyspace-events` includes expired, `Ex`/`Kx`) fires a `__keyevent@<db>__:expired` event. If enabled, the library MAY run an in-process subscriber on `__keyevent@*__:expired` (per node in Cluster) that, on any `rwlock:{r}:*` key expiry, calls `expire_and_grant` for that resource — promptly granting a waiter without a self-wake round-trip. (Readers live in a ZSET with no per-member TTL, so a crashed *reader* fires no event and is recovered by a waiting writer's self-wake; this is acceptable since a stale reader only reduces the count.) Caveats: expired events fire when Redis actually deletes the key (sampled active-expiry or on access), **not** exactly at TTL zero, so timing is approximate; in Cluster each node emits only for its own keyspace. We **never** call `CONFIG SET`; if events are off we silently use layer 2.

> An always-on "expiry dispatcher" with a global deadline index is deliberately **out of scope**: it would require leader election or sharded ownership to operate, it concentrates writes on a single global key (a hot slot that breaks the otherwise clean per-resource horizontal scaling), and — most importantly — it is infrastructure the user would have to run. A library must not require it. (Teams that want one can build it on top of `expire_and_grant`; we do not ship or require it.)

---

## 11. Fairness policies and writer-starvation prevention

A `fairness` setting, enforced entirely inside the scripts. It is persisted on each request and the **head request's policy governs** a grant decision; it is therefore expected to be **uniform per resource namespace** (mixing policies on one resource is unsupported — the head's wins). The policies:

- **`write_preferring` (default).** A queued writer blocks *new* readers from jumping ahead, even while readers currently hold. Existing readers drain, then the writer goes. This mirrors Go's `sync.RWMutex`, where a blocked `Lock` excludes new readers so the lock eventually becomes available. Prevents writer starvation without fully serializing.
- **`fifo`.** Strict queue order; contiguous readers at the head are still batched. Strongest fairness; slightly lower read throughput under mixed load.
- **`read_preferring`.** Readers proceed whenever no writer holds; maximizes read throughput but writers can starve. Only for read-heavy workloads with rare, non-urgent writes.

`max_reader_batch` (default 1000) caps how many readers a single grant may wake, bounding script runtime under extreme read fan-in. Remaining readers are granted on subsequent transitions.

---

## 12. Fencing tokens — the real correctness boundary

Every successful acquire returns `fencing` = a monotonically increasing integer from `INCR rwlock:{r}:fence`, unique and ordered per resource.

**How users enforce it (document and provide a helper):** the protected storage/service records the highest fencing token it has accepted for a resource and **rejects any mutation carrying a token ≤ that high-water mark**:

```
accept a write to resource r only if incoming_fencing > last_accepted_fencing[r]
then set last_accepted_fencing[r] = incoming_fencing
```

This is what makes the system safe even if Redis ever grants the lock to two clients (failover, or a paused holder whose lease lapsed): the stale holder's lower token is refused at the resource.

Notes:
- Tokens are issued to readers and writers alike; enforcement applies on **mutation** paths (readers typically don't present tokens because they don't mutate). Sharing one counter is fine.
- `enable_fencing` defaults to **true** (always issue). Enforcement is necessarily the user's responsibility at their resource — make it a one-liner in docs, not a footnote.

---

## 13. Public API contract + idiomatic examples

### 13.1 Language-neutral contract

```
LockMode = "read" | "write"

AcquireOptions {
  resource: string
  mode: LockMode
  leaseMs: number            # default 30000, max 300000
  waitMs: number             # default 10000,  max 60000
  ownerId?: string           # required by default
  fairness?: "write_preferring" | "fifo" | "read_preferring"   # default write_preferring
  watchdog?: boolean         # default false
  maxReaderBatch?: number    # default 1000
}

LockHandle {
  resource: string
  mode: LockMode
  token: string
  fencingToken: number
  leaseUntilMs: number
}

acquire(opts) -> LockHandle            # throws/returns WaitTimeout, BackendUnavailable
release(handle) -> void
extend(handle, leaseMs) -> LockHandle  # throws/returns LockLost
inspect(resource) -> ResourceStatus    # debugging (Section 18)
```

### 13.2 The scoped form is the documented front door

Raw `acquire`/`release`/`extend` exist for power users, but the **primary** documented API is the closure/scoped form, because a library cannot assume the caller has any framework lifecycle to clean up after them — the language's own `with` / `defer` / `try-finally` / `using` is the only release guarantee we can rely on. The scoped form guarantees release, ties cancellation to lock liveness, and (with watchdog) auto-extends.

**Node.js (npm):**
```javascript
import { RwLock } from "@org/redis-rwlock";
const rw = new RwLock(redis);   // wraps an existing ioredis / node-redis client

await rw.withWriteLock("order:123", { leaseMs: 30_000, watchdog: true }, async (signal, handle) => {
  // `signal` is an AbortSignal that fires the instant the lock is lost.
  await doWork(signal);
  await storage.write(payload, { fencingToken: handle.fencingToken });  // enforce fencing
});
// released automatically, even on throw
```

**Python (PyPI):**
```python
from redis_rwlock import RwLock
rw = RwLock(redis_client)   # wraps redis-py

async with rw.write_lock("order:123", lease_ms=30_000, watchdog=True) as lock:
    await do_work(lock.cancelled)          # `cancelled` trips if the lock is lost
    await storage.write(payload, fencing_token=lock.fencing_token)
# released on exit, including on exception
```

**Go (modules):**
```go
rw := rwlock.New(rdb) // wraps go-redis

err := rw.WithWriteLock(ctx, "order:123", rwlock.Opts{LeaseMs: 30000, Watchdog: true},
    func(ctx context.Context, h rwlock.Handle) error {
        // ctx is cancelled the moment the lock is lost
        if err := doWork(ctx); err != nil { return err }
        return storage.Write(ctx, payload, h.FencingToken)
    })
```

Read locks use the analogous `withReadLock` / `read_lock` / `WithReadLock`. A plain mutex is `withWriteLock` with no readers in play.

---

## 14. Error taxonomy

Distinct, catchable types per language (callers must be able to tell these apart):

```
WaitTimeout            could not acquire within waitMs (not an error condition for many callers)
BackendUnavailable     Redis unreachable / command failed -> fail closed; acquisition did NOT happen
LockLost               a held lock's lease expired or was taken (extend returned LOST, or watchdog detected it)
NotHeld / InvalidHandle release/extend on a token not currently held by the caller
IncompatibleServerLogic the installed script module's protocol_version is incompatible (Section 16)
Unsupported            a required capability is unavailable and no acceptable fallback exists (should be rare)
```

`LockLost` SHOULD also surface as cancellation of the scoped API's signal/context, not only as a thrown error from `extend`.

---

## 15. Connection management

The `BLPOP` design needs **one blocked connection per currently-waiting acquisition** (bounded by contention, not by throughput — uncontended high-QPS workloads use almost none). In a *library*, exhausting the user's shared pool would surface as unrelated timeouts they blame on us, so:

- Maintain a **dedicated, separate pool for blocking waits**; do not borrow from the user's main pool. Make its size configurable (`blockingPoolSize`).
- On pool exhaustion, an `acquire` with a wait MUST behave predictably and configurably: either wait for a blocking connection up to the remaining `waitMs`, or fail fast with a clear `BackendUnavailable`/`Unsupported`-style error. Never hang indefinitely or silently steal the main pool.
- Use one connection per blocked `BLPOP`; never multiplex other commands onto a connection blocked in `BLPOP`.
- Emit a gauge for in-use blocking connections (Section 18) so the most likely "your library broke my app" scenario is diagnosable.
- Document the per-waiter connection cost prominently.

If a deployment is connection-constrained but watches many locks, an optional alternative transport using **sharded pub/sub** (`SSUBSCRIBE`/`SPUBLISH`, Redis 7+) lets one connection watch many locks without the classic-pub/sub cluster-wide fan-out. This is an optional mode, not the default; the default is `BLPOP` mailboxes.

---

## 16. Versioning and cross-version compatibility

Once published you cannot recall a release, and **mixed client versions will contend on the same locks in the same Redis** (one service upgrades before another). So:

- The Lua/Functions module embeds `PROTOCOL_VERSION` (a **single integer**, currently **1**) and writes `rwlock:__module__ = { protocol_version, impl_version, sha, loaded_at_ms }` when installed. `loaded_at_ms` is Redis server time (§5); `impl_version` is informational and never used in compatibility decisions.
- **Compatibility is exact-integer equality** on `protocol_version`. On connect the client reads `rwlock:__module__`; if it exists and its `protocol_version` differs from the client's, the client raises `IncompatibleServerLogic` rather than silently contending under foreign semantics. (A config flag may allow coexistence only when explicitly opted into.) There is no major/minor split — every protocol change bumps the integer and is mutually incompatible by construction.
- **Any change to the wire protocol, key schema, script return contracts, or grant semantics bumps `PROTOCOL_VERSION`.** Clients with the same `PROTOCOL_VERSION` are guaranteed identical semantics and are safe to contend; different versions must not share a resource namespace.
- **Install algorithm (normative).** On connect: (1) attempt `FUNCTION LOAD REPLACE <library>` (idempotent; on failure fall back to `SCRIPT LOAD`/`EVALSHA` — Section 17); (2) read `rwlock:__module__`; if absent, `HSET` it. `FUNCTION LOAD REPLACE` and the marker write are each individually idempotent and safe under concurrent installers (all write identical values). If a crash leaves the module loaded but the marker absent, the next client simply writes the marker. The client must never assume the marker's presence implies its *own* version is loaded — it always (re)loads its library, then checks the marker for an incompatible peer.

Document the cross-version contention guarantee explicitly in the README.

---

## 17. Capability detection and graceful degradation

We do not own the Redis. Probe on connect and adapt; **never require an admin command to function.**

| Capability | Probe | If present | If absent / denied |
|---|---|---|---|
| Redis Functions | `FUNCTION LIST` / attempt `FUNCTION LOAD` | install & `FCALL` | fall back to `SCRIPT LOAD` + `EVALSHA` (same Lua source) |
| Keyspace expiry events | read `CONFIG GET notify-keyspace-events` if permitted | optionally run the in-process expiry subscriber | use per-waiter self-wake (Section 10.2); never `CONFIG SET` |
| Topology | `INFO` / `CLUSTER INFO` | standalone/Sentinel/Cluster paths | — |
| Cluster hash slots | detect Cluster | enforce `{r}` co-location; route per node | — |
| RESP3 | `HELLO 3` if available | use push messages where helpful | RESP2 fine |

Degradation must be **silent and automatic** — a managed Redis that forbids `FUNCTION LOAD` or `CONFIG` must still yield a fully working lock (via `EVALSHA` + self-wake). A throw in that situation is a one-star review.

---

## 18. Observability

**Metrics (emit via the language's standard metrics hooks):**
```
rwlock_acquire_total{mode,result}        # result = granted | timeout | error
rwlock_wait_duration_ms{mode}            # histogram
rwlock_held_duration_ms{mode}            # histogram
rwlock_queue_length{resource}            # gauge / sampled
rwlock_queued_writers{resource}
rwlock_timeouts_total{mode}
rwlock_expired_holders_total             # leading indicator of crashes/timeouts in prod
rwlock_extend_total{result}
rwlock_release_total{result}
rwlock_lock_lost_total{mode}
rwlock_blocking_connections_in_use       # gauge (see Section 15)
rwlock_fencing_token_current{resource}
```

**Tracing:** spans around `acquire` (with child spans for `wait` and `hold`), `extend`, `release`; record `resource`, `mode`, `fencing`, outcome.

**Debug command:** `inspect(resource)` (via a read-only script) returns:
```json
{ "mode": "read", "readerCount": 12, "writerActive": false,
  "queueLength": 8, "queuedWriters": 1, "oldestWaitMs": 2300, "nextExpiryMs": 1700 }
```

**Logging:** structured; never log tokens at info level.

---

## 19. Configuration and defaults

```
fairness                = "write_preferring"      # default; matches Go sync.RWMutex
default_lease_ms        = 30000
max_lease_ms            = 300000
default_wait_ms         = 10000
max_wait_ms             = 60000
watchdog                = false                   # opt-in; refresh at lease/3 when on
extension_margin_ms     = max(500, p99_redis_latency_ms * 4)
max_reader_batch        = 1000
notify_key_ttl_ms       = 60000
request_key_ttl_grace_ms= 60000                   # request TTL = wait_ms + this
enable_fencing          = true
require_owner_id        = true
blocking_pool_size      = configurable (e.g. 16, with backpressure on exhaustion)
time_source             = redis_server (TIME)     # never client clock
transport               = "blpop"                 # alt: "sharded_pubsub" (Redis 7+)
keyspace_events         = "auto"                   # use if enabled; never CONFIG SET
```

All limits are enforced **server-side, in the Lua** (the brain clamps `lease_ms`, `wait_ms`, and `max_reader_batch` to the protocol limits), so a misbehaving or non-conforming client cannot impose an out-of-range lease that other clients must honor. Clients SHOULD also clamp locally for fast feedback, but the brain's clamp is authoritative. `notify_key_ttl_ms` and `request_key_ttl_grace_ms` are transmitted per call (Section 7), not read from server-side config.

---

## 20. Testing and acceptance criteria

Nobody bets a production system on a lock because the README looks nice. The test suite is the credibility. A component is "done" only when its tests below pass.

### 20.1 Safety invariants (property-based, with fault injection)

Run randomized schedules of concurrent readers/writers (and induced delays, dropped connections, killed clients) and assert the invariants on every observed state:
- never two writers concurrently;
- never a reader concurrent with a writer;
- a granted writer always observed alone;
- FIFO order respected, with contiguous reader batching;
- under a continuous flood of readers, a waiting writer is eventually granted (no starvation) in `write_preferring` and `fifo`.

### 20.2 Liveness / handoff

- A release wakes the next eligible waiter "immediately" (bounded by RTT), with **no acquire re-attempt** observed on the wire (assert no polling).
- Contiguous readers are batch-granted in one release.

### 20.3 Crash & recovery

- Kill a holder process; assert the next waiter acquires within `lease_ms` (+ event/self-wake slack). Test both with keyspace events enabled and disabled.
- Crashed holder with no waiters: assert the next acquirer reclaims it via lazy cleanup.

### 20.4 Failover & fencing

- Force a primary→replica failover mid-hold; assert that the stale fencing token is **rejected by a test resource** (proving fencing protects correctness even when the lock is briefly double-granted).

### 20.5 Ghost-grant race

- Engineer a wait timeout that coincides with a grant; assert the lock is **either taken by that caller or fully reclaimed**, never leaked, and never held by a vanished owner past its lease.

### 20.6 Watchdog & extension

- Long operation with watchdog keeps the lock; stopping the watchdog (or killing the holder) results in `LockLost` and a cancelled signal/context.
- Extension within the safety margin succeeds; extension attempted after expiry returns `LOST`.

### 20.7 Connection hygiene

- Spawn many concurrent waiters; assert blocking connections are released on grant, timeout, and cancellation (no leak); assert the user's main pool is untouched.

### 20.8 Degradation & compatibility

- Full suite passes against Redis with Functions disabled (forces `EVALSHA`) and with keyspace events disabled (forces self-wake).
- Cross-version: a client of the same MAJOR contends correctly; a client of an incompatible MAJOR raises `IncompatibleServerLogic`.

### 20.9 Optional formal model

A TLA+ / FizzBee model of the state machine asserting the safety invariants is recommended for the core; it is the artifact that turns "looks reasonable" into "we trust it in prod."

### 20.10 Conformance suite (cross-language)

Maintain a **language-agnostic scenario file** plus the shared Lua. The normative format and runner contract live in `protocol/conformance/README.md`; a port is "done" only when it passes the shared scenarios. The current format is **sequential** (single logical client) and pins immediate-grant / queue-then-timeout / hand-off / fencing monotonicity. Proving real interop additionally requires **concurrent** scenarios (per-step `client` ids, a barrier/ordering construct, and `inspect`-based state assertions) and a **mixed-language** test (e.g. a Go writer contending with a Python reader); these and the runner contract are introduced with the second-language port. Until then, treat the cross-language interop claim (§0) as *designed-for* but not yet *proven*.

---

## 21. Build plan / milestones

Implement the **reference language first** (recommend Node.js or Python for fastest iteration; Go is an easy port after). Each milestone ends with its slice of the Section 20 tests green.

- **M0** Single-node **write lock**: `acquire`/`release`, lease, token verification, fencing, `BLPOP` mailbox, fail-closed, server `TIME`.
- **M1** **Read locks** + `grant_contiguous_readers` batching + derived state (`readers`/`writer`).
- **M2** FIFO **queue** (ZSET-by-seq) + `fairness` policies + `cancel_wait` (incl. ghost-grant reconciliation).
- **M3** `extend` + safety margin + opt-in **watchdog** + scoped API with cancellation.
- **M4** **Recovery**: lazy cleanup + self-wake + optional keyspace-event subscriber (`expire_and_grant`).
- **M5** **Cluster** hash-tagging + **Functions-or-EVALSHA** delivery + **capability probe** + **version** handshake.
- **M6** **Observability** (metrics, tracing, `inspect`) + connection-pool ergonomics/backpressure.
- **M7** **Second language port** over the same Lua + **cross-language conformance suite**.

---

## 22. Per-language porting guide

**Shared and identical across all languages (do not reimplement):**
- The Lua source for all five scripts + `grant_from_queue` (Appendix A).
- The key schema (Section 4), payload/handle formats (Appendix B), `PROTOCOL_VERSION`.
- Script return contracts (Section 7) and the wait algorithm's *logic* (Section 8).
- Defaults and limits (Section 19).

**Per language (idiomatic):**
- Public API surface and the scoped form using native lifecycle (`try-finally`/`with`/`defer`/`using`) and native cancellation (`AbortSignal` / `context.Context` / `asyncio` cancellation / `Future`).
- Wrapping the language's standard Redis client; connection pooling, including the dedicated blocking pool.
- Error types (Section 14) mapped to native conventions.
- Async/concurrency model (event loop vs goroutines vs threads); the watchdog timer.
- Metrics/tracing wired to the ecosystem's standard libraries.

**Rule of thumb:** if a behavior could differ between two correct clients and affect who gets the lock, it belongs in the Lua and is shared. If it is only about ergonomics or I/O plumbing, it is per-language.

---

## Appendix A: full script pseudocode

> Pseudocode, not drop-in Lua, but precise about ordering and effects. `now = redis.TIME()` at the top of each mutating script. `KEYS[1]` is the prefix `rwlock:{r}`; all keys are derived from it. State is **derived from truth** (`readers`/`writer`/`queue`), never cached.

### A.1 Shared helpers

```
sweep(r, now):
    ZREMRANGEBYSCORE readers 0 now                       # evict expired readers
    if EXISTS writer and writer.expire_at_ms <= now: DEL writer

writer_held(r)   -> EXISTS writer                         # call after sweep
reader_count(r)  -> ZCARD readers                         # call after sweep

# Drop timed-out and orphaned queue entries ANYWHERE in the queue, and RETURN the
# number of live queued writers — derived from the surviving req hashes, never an
# incremental counter (which drifts permanently on a crashed queued writer).
prune_queue(r, now) -> queued_writers:
    writers = 0
    for id in ZRANGE queue 0 -1:
        wd = req[id].wait_deadline_ms ; gt = req[id].granted_token
        if wd is nil:                 ZREM queue id                 # orphan: req hash gone
        elif wd <= now and gt == "":  ZREM queue id ; DEL req[id]   # timed out, not granted
        elif req[id].mode == "write": writers += 1
    return writers

# Soonest absolute time something actionable could happen for a waiter.
next_wake(r, now) -> ms:
    best = min over { soonest readers score, writer.expire_at_ms, head req.wait_deadline_ms }
    return best or -1

push_grant(req_id, token, fencing, expire_at, mode, now):
    payload = json{status:"GRANTED", token, fencing, lease_until_ms:expire_at, mode}
    LPUSH req[req_id].notify_key payload
    PEXPIRE req[req_id].notify_key max(notify_key_ttl_ms, expire_at - now)

new_token(owner_id, req_id, fencing) -> owner_id ":" req_id ":" fencing

grant_writer(req_id, now):
    fencing = INCR fence ; token = new_token(req[req_id].owner_id, req_id, fencing)
    expire_at = now + req[req_id].lease_ms
    HSET writer {token, expire_at_ms: expire_at} ; PEXPIRE writer req[req_id].lease_ms
    HSET req[req_id].granted_token = token
    push_grant(req_id, token, fencing, expire_at, "write", now) ; ZREM queue req_id

grant_reader(id, now):
    fencing = INCR fence ; token = new_token(req[id].owner_id, id, fencing)
    expire_at = now + req[id].lease_ms
    ZADD readers expire_at token
    HSET req[id].granted_token = token
    push_grant(id, token, fencing, expire_at, "read", now) ; ZREM queue id

grant_contiguous_readers(r, now) -> count:          # write_preferring / fifo
    count = 0
    while head h exists and req[h].mode == "read" and count < req[h].max_reader_batch:
        grant_reader(h, now) ; count += 1
    return count

grant_readers_anywhere(r, now) -> count:            # read_preferring (skips queued writers)
    count = 0
    for id in ZRANGE queue 0 -1:
        if req[id].mode == "read" and count < req[id].max_reader_batch:
            grant_reader(id, now) ; count += 1
    return count
```

### A.2 `grant_from_queue`

```
grant_from_queue(r, now) -> granted:
    sweep(r, now) ; prune_queue(r, now)
    if writer_held(r): return 0
    h = queue head ; if h is nil: return 0
    fairness = req[h].fairness
    if reader_count(r) > 0:
        if fairness == "read_preferring": return grant_readers_anywhere(r, now)
        return grant_contiguous_readers(r, now)
    if fairness == "read_preferring":
        g = grant_readers_anywhere(r, now) ; if g > 0: return g
        if req[h].mode == "write": grant_writer(h, now); return 1
        return 0
    if req[h].mode == "write": grant_writer(h, now); return 1
    return grant_contiguous_readers(r, now)
```

### A.3 `acquire`

```
acquire(r, mode, lease_ms, wait_ms, request_id, owner_id, fairness, max_reader_batch,
        notify_key_ttl_ms, request_key_ttl_grace_ms):
    now = redis.TIME()
    lease_ms = clamp(lease_ms, 1, MAX_LEASE_MS) ; wait_ms = clamp(wait_ms, 0, MAX_WAIT_MS)
    max_reader_batch = clamp(max_reader_batch, 1, MAX_READER_BATCH)
    sweep(r, now)
    queued_writers = prune_queue(r, now)
    grantable =
        (mode == "read" and not writer_held(r) and
            (fairness == "read_preferring"
             or (fairness == "fifo" and queue is empty)
             or queued_writers == 0))                      # write_preferring
     or (mode == "write" and not writer_held(r) and reader_count(r) == 0 and queue is empty)

    if grantable:
        fencing = INCR fence ; token = new_token(owner_id, request_id, fencing)
        expire_at = now + lease_ms
        if mode == "write": HSET writer {token, expire_at_ms: expire_at}; PEXPIRE writer lease_ms
        else:               ZADD readers expire_at token
        return ["GRANTED", token, fencing, expire_at, mode]

    # enqueue (no req hash on the immediate path above; only here)
    seq = INCR seq
    HSET req[request_id] = {mode, owner_id, lease_ms, wait_deadline_ms: now+wait_ms,
        notify_key: "rwlock:{r}:notify:"+request_id, granted_token:"", created_at_ms: now,
        fairness, max_reader_batch}
    PEXPIRE req[request_id] (wait_ms + request_key_ttl_grace_ms)
    ZADD queue seq request_id
    return ["QUEUED", request_id, req.notify_key, now+wait_ms, next_wake(r, now)]
```

### A.4 `release`

```
release(token):
    now = redis.TIME() ; sweep(r, now)
    if EXISTS writer and writer.token == token: DEL writer
    elif ZSCORE readers token is not nil:        ZREM readers token
    else: return ["NOT_HELD"]
    grant_from_queue(r, now)
    return ["OK"]
```

### A.5 `extend`

```
extend(token, lease_ms):
    now = redis.TIME() ; lease_ms = clamp(lease_ms, 1, MAX_LEASE_MS)
    if EXISTS writer and writer.token == token:
        if writer.expire_at_ms <= now: return ["LOST"]
        new = max(now + lease_ms, writer.expire_at_ms)        # never shorten
        HSET writer.expire_at_ms = new ; PEXPIRE writer (new - now) ; return ["OK", new]
    score = ZSCORE readers token
    if score and score > now:
        new = max(now + lease_ms, score) ; ZADD readers new token ; return ["OK", new]
    return ["LOST"]
```

### A.6 `cancel_wait`

```
cancel_wait(request_id):
    now = redis.TIME()
    if req[request_id] does not exist: ZREM queue request_id ; return ["GONE"]
    granted = req[request_id].granted_token
    if granted == "":
        was_head = (queue head == request_id)
        ZREM queue request_id ; DEL req[request_id]
        if was_head: grant_from_queue(r, now)
        return ["CANCELLED"]
    # granted at the last instant -> release that holder (writer or reader), hand on
    if EXISTS writer and writer.token == granted: DEL writer
    elif ZSCORE readers granted is not nil:       ZREM readers granted
    DEL req[request_id]
    grant_from_queue(r, now)
    return ["RECLAIMED", granted]
```

### A.7 `expire_and_grant`

```
expire_and_grant(r):
    now = redis.TIME()
    granted = grant_from_queue(r, now)   # sweep + prune happen inside
    return ["OK", granted, next_wake(r, now)]
```

---

## Appendix B: payload and handle formats

Token grammar (§5): `"<owner_id>:<request_id>:<fencing>"`, e.g. `"worker-1:01J9Z…ULID:918273"`.

**Grant payload** (pushed to `notify:{id}`, JSON string — snake_case on the wire):
```json
{ "status": "GRANTED", "token": "worker-1:01J9Z...:918273", "fencing": 918273,
  "lease_until_ms": 1760000000000, "mode": "write" }
```

**Handle** (returned to the caller — camelCase in the public API). Wire→handle field mapping:
`token`→`token`, `fencing`→`fencingToken`, `lease_until_ms`→`leaseUntilMs`, `mode`→`mode`, plus `resource`.
```json
{ "resource": "order:123", "mode": "write", "token": "worker-1:01J9Z...:918273",
  "fencingToken": 918273, "leaseUntilMs": 1760000000000 }
```

**Module marker** (`rwlock:__module__`, written on install). `loaded_at_ms` is Redis server time; `impl_version` is informational (not used for compatibility):
```json
{ "protocol_version": 1, "impl_version": "0.0.0", "sha": "<function-library-sha1>",
  "loaded_at_ms": 1760000000000 }
```

---

## Appendix C: glossary

- **Holder** — a client currently holding the lock (a reader, or the single writer).
- **Waiter / request** — a queued acquisition attempt blocked on its mailbox.
- **Lease** — the absolute expiry on a holder; the deadlock backstop.
- **Mailbox** — `rwlock:{r}:notify:{id}`, the per-waiter list a `BLPOP` blocks on.
- **Grant / handoff** — pushing the lock to a waiter's mailbox inside the freeing operation.
- **Fencing token** — monotonic per-resource integer enforced at the protected resource for correctness.
- **Self-wake** — a waiter's bounded re-evaluation at a holder's lease boundary to detect a crash; not polling.
- **Lazy cleanup** — eviction of expired holders at the start of every script.
- **`grant_from_queue`** — the shared routine that decides who gets woken next.

---

*End of specification. The server-side contract (Sections 4–9, Appendix A/B) is identical across all languages and is the single source of truth; verify it with the conformance suite (Section 20.10) before relying on any client.*
