import { randomUUID } from "node:crypto";
import { createClientPool, type RedisClientPoolType, type RedisClientType } from "redis";
import { ScriptRunner } from "./scripts.js";
import { LockHandle, type HandleOwner } from "./handle.js";
import { KeyspaceSubscriber, type SubscriberConn } from "./keyspace.js";
import { BackendUnavailableError, LockLostError, WaitTimeoutError } from "./errors.js";
import {
  DEFAULTS,
  type AcquireOptions,
  type Fairness,
  type LockMode,
  type RwLockConfig,
} from "./types.js";

// Self-wake tuning for the wait loop (SPEC §8.2). These bound the BLPOP block so a
// waiter re-evaluates at a crashed holder's lease boundary without ever busy-spinning.
const EPSILON_MS = 50; // wake just past a holder's lease boundary
const FLOOR_MS = 50; // never block for less than this (avoids tight loops / a 0 = infinite BLPOP)
const SIGNAL_POLL_MS = 250; // when a cancel signal is supplied, re-check at least this often

// Accept any node-redis client; users' clients differ in module/script generics.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type RedisClientLike = RedisClientType<any, any, any, any, any>;

/** Callback for the scoped API: receives the live lock (with `.signal`). */
export type LockScope<T> = (lock: LockHandle) => Promise<T> | T;

/** "rwlock:{<resource>}" — the cluster hash tag keeps every key on one slot. */
export function keyPrefix(resource: string): string {
  return `rwlock:{${resource}}`;
}

function newRequestId(): string {
  // Roughly time-sortable; a ULID/UUIDv7 would be ideal but is not required.
  return `${Date.now().toString(36)}-${randomUUID()}`;
}

function abortError(signal: AbortSignal): Error {
  const reason = signal.reason as unknown;
  if (reason instanceof Error) return reason;
  const e = new Error("acquire aborted");
  e.name = "AbortError";
  return e;
}

interface GrantPayload {
  status: "GRANTED";
  token: string;
  fencing: number;
  lease_until_ms: number;
  mode: LockMode;
}

/**
 * Distributed read/write lock over Redis. Wraps an existing node-redis client; all
 * lock decisions live in the shared Lua, so this class is a thin transport layer.
 *
 * Provides the raw acquire/release/extend methods, the scoped front-door `withWriteLock`/
 * `withReadLock` (guaranteed release + cancellation + optional watchdog), and
 * AsyncDisposable handles for `await using`. Cluster routing, the Functions
 * delivery path, and observability arrive in later milestones.
 */
export class RwLock {
  private readonly client: RedisClientLike;
  private readonly scripts: ScriptRunner;
  private readonly cfg: Required<RwLockConfig>;
  private ready?: Promise<void>;
  private serverTimeOffsetMs = 0;
  // Dedicated pool for blocking BLPOP waits so we never tie up the user's client (SPEC §15).
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private blockingPool?: RedisClientPoolType<any, any, any, any, any>;
  private keyspace?: KeyspaceSubscriber;
  private readonly owner: HandleOwner;

  constructor(client: RedisClientLike, config: RwLockConfig = {}) {
    this.client = client;
    this.scripts = new ScriptRunner(client);
    this.cfg = { ...DEFAULTS, ...config };
    this.owner = {
      releaseToken: (resource, token) => this.releaseToken(resource, token),
      extendToken: (resource, token, leaseMs) => this.extendToken(resource, token, leaseMs),
      serverNow: () => this.serverNow(),
      extensionMarginMs: this.cfg.extensionMarginMs,
      defaultLeaseMs: this.cfg.defaultLeaseMs,
    };
  }

  /** Connectivity check, clock-offset learning, and blocking-pool setup — once, lazily. */
  private ensureReady(): Promise<void> {
    if (!this.ready) {
      this.ready = (async () => {
        const t = (await this.client.sendCommand(["TIME"])) as [string, string];
        const serverMs = Number(t[0]) * 1000 + Math.floor(Number(t[1]) / 1000);
        this.serverTimeOffsetMs = serverMs - Date.now();

        const pool = createClientPool(this.client.options, {
          maximum: this.cfg.blockingPoolSize,
          acquireTimeout: this.cfg.maxWaitMs,
        });
        pool.on("error", () => {
          /* swallow: surfaced per-call as BackendUnavailableError */
        });
        await pool.connect();
        this.blockingPool = pool;

        // Optional recovery accelerator: subscribe to keyspace expiry events if the
        // server has them enabled (auto-detected; we never enable them ourselves).
        if (this.cfg.keyspaceEvents !== "off" && (await this.detectKeyspaceEvents())) {
          const sub = new KeyspaceSubscriber(
            () => this.client.duplicate() as unknown as SubscriberConn,
            async (resource) => {
              await this.scripts.run("expireAndGrant", [keyPrefix(resource)], [
                String(this.cfg.notifyKeyTtlMs),
              ]);
              this.cfg.onRecovery(resource);
            },
          );
          try {
            await sub.start();
            this.keyspace = sub;
          } catch {
            /* degrade silently to the self-wake path */
          }
        }
      })().catch((err) => {
        this.ready = undefined; // allow a later retry
        throw new BackendUnavailableError("failed to initialise against Redis", { cause: err });
      });
    }
    return this.ready;
  }

  /** Best-effort estimate of the Redis server clock; used only to size timeouts. */
  private serverNow(): number {
    return Date.now() + this.serverTimeOffsetMs;
  }

  /** Whether the keyspace-expiry subscriber is currently running. */
  get keyspaceActive(): boolean {
    return this.keyspace?.active ?? false;
  }

  /** Read the server's notify-keyspace-events flags; true iff expired keyevents fire. */
  private async detectKeyspaceEvents(): Promise<boolean> {
    try {
      const reply = (await this.client.sendCommand(["CONFIG", "GET", "notify-keyspace-events"])) as unknown;
      let flags = "";
      if (Array.isArray(reply)) flags = String(reply[1] ?? "");
      else if (reply && typeof reply === "object") {
        flags = String((reply as Record<string, unknown>)["notify-keyspace-events"] ?? "");
      }
      // need keyevent notifications (E) and expired events (x, or A which includes x)
      return /E/.test(flags) && (/x/.test(flags) || /A/.test(flags));
    } catch {
      return false; // CONFIG denied (e.g. managed Redis) -> use the self-wake path
    }
  }

  /** Release the blocking pool and keyspace subscriber. Does NOT touch the user's client. */
  async close(): Promise<void> {
    const sub = this.keyspace;
    this.keyspace = undefined;
    if (sub) await sub.stop().catch(() => {});

    const pool = this.blockingPool;
    this.blockingPool = undefined;
    this.ready = undefined;
    if (pool) {
      try {
        await pool.close();
      } catch {
        /* already closed */
      }
    }
  }

  acquireWrite(resource: string, opts: AcquireOptions = {}): Promise<LockHandle> {
    return this.acquire(resource, "write", opts);
  }

  acquireRead(resource: string, opts: AcquireOptions = {}): Promise<LockHandle> {
    return this.acquire(resource, "read", opts);
  }

  // --- Scoped API: the documented front door (guaranteed release + cancellation). ---

  withWriteLock<T>(resource: string, fn: LockScope<T>): Promise<T>;
  withWriteLock<T>(resource: string, opts: AcquireOptions, fn: LockScope<T>): Promise<T>;
  withWriteLock<T>(resource: string, b: AcquireOptions | LockScope<T>, c?: LockScope<T>): Promise<T> {
    return this.withLock(resource, "write", b, c);
  }

  withReadLock<T>(resource: string, fn: LockScope<T>): Promise<T>;
  withReadLock<T>(resource: string, opts: AcquireOptions, fn: LockScope<T>): Promise<T>;
  withReadLock<T>(resource: string, b: AcquireOptions | LockScope<T>, c?: LockScope<T>): Promise<T> {
    return this.withLock(resource, "read", b, c);
  }

  private async withLock<T>(
    resource: string,
    mode: LockMode,
    b: AcquireOptions | LockScope<T>,
    c?: LockScope<T>,
  ): Promise<T> {
    const opts = typeof b === "function" ? {} : b;
    const fn = (typeof b === "function" ? b : c) as LockScope<T>;
    const lock = await this.acquire(resource, mode, opts);
    try {
      return await fn(lock);
    } finally {
      await lock.release().catch(() => {
        /* release is best-effort in the scope's finally; loss already surfaced via signal */
      });
    }
  }

  async acquire(resource: string, mode: LockMode, opts: AcquireOptions = {}): Promise<LockHandle> {
    if (opts.signal?.aborted) throw abortError(opts.signal);
    await this.ensureReady();
    const o = this.resolveOptions(opts);
    const prefix = keyPrefix(resource);
    const requestId = newRequestId();

    let res: unknown[];
    try {
      res = (await this.scripts.run(
        "acquire",
        [prefix],
        [
          mode,
          String(o.leaseMs),
          String(o.waitMs),
          requestId,
          o.ownerId,
          o.fairness,
          String(o.maxReaderBatch),
          String(this.cfg.notifyKeyTtlMs),
          String(this.cfg.requestKeyTtlGraceMs),
        ],
      )) as unknown[];
    } catch (err) {
      throw new BackendUnavailableError(`acquire failed for ${resource}`, { cause: err });
    }

    if (String(res[0]) === "GRANTED") {
      return this.makeHandle(
        {
          resource,
          mode: String(res[4]) as LockMode,
          token: String(res[1]),
          fencingToken: Number(res[2]),
          leaseUntilMs: Number(res[3]),
        },
        o,
      );
    }

    // QUEUED -> block on the private mailbox.
    const notifyKey = String(res[2]);
    const waitDeadlineMs = Number(res[3]);
    const headHolderLeaseMs = Number(res[4]);
    return this.waitForGrant(resource, prefix, requestId, notifyKey, waitDeadlineMs, headHolderLeaseMs, o);
  }

  private async waitForGrant(
    resource: string,
    prefix: string,
    requestId: string,
    notifyKey: string,
    waitDeadlineMs: number,
    headHolderLeaseMs: number,
    o: ResolvedOptions,
  ): Promise<LockHandle> {
    let holderLeaseMs = headHolderLeaseMs;
    for (;;) {
      if (o.signal?.aborted) {
        await this.abortPending(prefix, requestId);
        throw abortError(o.signal);
      }
      const remaining = waitDeadlineMs - this.serverNow();
      if (remaining <= 0) {
        return this.finishTimeout(resource, prefix, requestId, notifyKey, o);
      }

      // Block until the holder's lease boundary (to catch a crash) or the deadline,
      // re-checking a cancel signal at least every SIGNAL_POLL_MS.
      const boundary = holderLeaseMs > 0 ? holderLeaseMs - this.serverNow() + EPSILON_MS : remaining;
      let blockMs = Math.max(FLOOR_MS, Math.min(remaining, Math.max(boundary, 0) || remaining));
      if (o.signal) blockMs = Math.min(blockMs, SIGNAL_POLL_MS);

      let popped: { key: unknown; element: unknown } | null;
      try {
        // A dedicated, pooled blocking connection (SPEC §15) — never the user's client.
        popped = await this.blockingPool!.execute((cli) => cli.blPop(notifyKey, blockMs / 1000));
      } catch (err) {
        throw new BackendUnavailableError(`wait failed for ${resource}`, { cause: err });
      }
      if (popped) {
        return this.handleFromPayload(resource, String(popped.element), o);
      }

      // Woke with no grant -> the holder may have crashed. Run maintenance once
      // (which may push a grant into our mailbox), refresh the boundary, re-block.
      try {
        await this.scripts.run("expireAndGrant", [prefix], [String(this.cfg.notifyKeyTtlMs)]);
      } catch (err) {
        throw new BackendUnavailableError(`wait maintenance failed for ${resource}`, { cause: err });
      }
      holderLeaseMs = await this.peekHeadHolderLease(prefix);
    }
  }

  /** Last-instant reconciliation when the wait deadline elapses (SPEC §8.4). */
  private async finishTimeout(
    resource: string,
    prefix: string,
    requestId: string,
    notifyKey: string,
    o: ResolvedOptions,
  ): Promise<LockHandle> {
    let drained: unknown;
    try {
      drained = await this.client.lPop(notifyKey); // granted at the buzzer?
    } catch (err) {
      throw new BackendUnavailableError(`wait drain failed for ${resource}`, { cause: err });
    }
    if (drained) {
      return this.handleFromPayload(resource, String(drained), o);
    }
    await this.abortPending(prefix, requestId);
    throw new WaitTimeoutError(`acquire timed out for ${resource}`);
  }

  /** Remove our queued request and reconcile a last-instant grant (CANCELLED/RECLAIMED/GONE). */
  private async abortPending(prefix: string, requestId: string): Promise<void> {
    try {
      await this.scripts.run("cancelWait", [prefix], [requestId, String(this.cfg.notifyKeyTtlMs)]);
    } catch (err) {
      throw new BackendUnavailableError(`cancel_wait failed`, { cause: err });
    }
  }

  private async peekHeadHolderLease(prefix: string): Promise<number> {
    try {
      const r = await this.client.zRangeWithScores(`${prefix}:holders`, 0, 0);
      return r.length > 0 ? Number(r[0]!.score) : -1;
    } catch {
      return -1; // a transient read failure just means we re-block for the remaining time
    }
  }

  private handleFromPayload(resource: string, payload: string, o: ResolvedOptions): LockHandle {
    const g = JSON.parse(payload) as GrantPayload;
    return this.makeHandle(
      {
        resource,
        mode: g.mode,
        token: g.token,
        fencingToken: g.fencing,
        leaseUntilMs: g.lease_until_ms,
      },
      o,
    );
  }

  private makeHandle(
    data: { resource: string; mode: LockMode; token: string; fencingToken: number; leaseUntilMs: number },
    o: ResolvedOptions,
  ): LockHandle {
    const handle = new LockHandle(this.owner, data);
    if (o.watchdog) handle.startWatchdog(o.leaseMs);
    return handle;
  }

  /** Release a held lock. NOT_HELD is a benign no-op (the lease may have expired). */
  async release(handle: LockHandle): Promise<void> {
    await handle.release();
  }

  /** Renew a held lease. Throws LockLostError if the lock is no longer ours. */
  extend(handle: LockHandle, leaseMs?: number): Promise<LockHandle> {
    return handle.extend(leaseMs);
  }

  // --- HandleOwner primitives (script-level; used by LockHandle) ---

  private async releaseToken(resource: string, token: string): Promise<void> {
    await this.ensureReady();
    try {
      await this.scripts.run(
        "release",
        [keyPrefix(resource)],
        [token, String(this.cfg.notifyKeyTtlMs)],
      );
    } catch (err) {
      throw new BackendUnavailableError(`release failed for ${resource}`, { cause: err });
    }
  }

  private async extendToken(resource: string, token: string, leaseMs: number): Promise<number> {
    await this.ensureReady();
    const lease = this.clampLease(leaseMs);
    let res: unknown[];
    try {
      res = (await this.scripts.run("extend", [keyPrefix(resource)], [token, String(lease)])) as unknown[];
    } catch (err) {
      throw new BackendUnavailableError(`extend failed for ${resource}`, { cause: err });
    }
    if (String(res[0]) === "LOST") {
      throw new LockLostError(`lock lost for ${resource}`);
    }
    return Number(res[1]);
  }

  private clampLease(leaseMs: number): number {
    return Math.max(1, Math.min(this.cfg.maxLeaseMs, Math.floor(leaseMs)));
  }

  private resolveOptions(opts: AcquireOptions): ResolvedOptions {
    const ownerId = opts.ownerId ?? "";
    if (this.cfg.requireOwnerId && ownerId === "") {
      throw new TypeError("ownerId is required (set requireOwnerId: false to opt out)");
    }
    return {
      leaseMs: this.clampLease(opts.leaseMs ?? this.cfg.defaultLeaseMs),
      waitMs: Math.max(0, Math.min(this.cfg.maxWaitMs, Math.floor(opts.waitMs ?? this.cfg.defaultWaitMs))),
      ownerId,
      fairness: opts.fairness ?? this.cfg.defaultFairness,
      maxReaderBatch: opts.maxReaderBatch ?? this.cfg.maxReaderBatch,
      watchdog: opts.watchdog ?? false,
      signal: opts.signal,
    };
  }
}

interface ResolvedOptions {
  leaseMs: number;
  waitMs: number;
  ownerId: string;
  fairness: Fairness;
  maxReaderBatch: number;
  watchdog: boolean;
  signal?: AbortSignal;
}
