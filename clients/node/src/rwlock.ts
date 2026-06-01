import { randomUUID } from "node:crypto";
import type { Redis } from "ioredis";
import { defineScripts, type RwlockRedis } from "./scripts.js";
import { BackendUnavailable, LockLost, WaitTimeout } from "./errors.js";
import {
  DEFAULTS,
  type AcquireOptions,
  type Fairness,
  type LockHandle,
  type LockMode,
  type RwLockConfig,
} from "./types.js";

// Self-wake tuning for the wait loop (Spec §8.2). These bound the BLPOP block so a
// waiter re-evaluates at a crashed holder's lease boundary without ever busy-spinning.
const EPSILON_MS = 50; // wake just past a holder's lease boundary
const FLOOR_MS = 50; // never block for less than this (avoids tight loops / a 0 = infinite BLPOP)

/** "rwlock:{<resource>}" — the cluster hash tag keeps every key on one slot. */
export function keyPrefix(resource: string): string {
  return `rwlock:{${resource}}`;
}

function newRequestId(): string {
  // Roughly time-sortable; a ULID/UUIDv7 would be ideal but is not required for M0.
  return `${Date.now().toString(36)}-${randomUUID()}`;
}

interface GrantPayload {
  status: "GRANTED";
  token: string;
  fencing: number;
  lease_until_ms: number;
  mode: LockMode;
}

/**
 * Distributed read/write lock over Redis. Wraps an existing ioredis client; all
 * lock decisions live in the shared Lua, so this class is a thin transport layer.
 *
 * M0 scope: raw acquire/release/extend for read & write locks, the BLPOP mailbox
 * wait loop with bounded self-wake, fencing, and fail-closed behaviour. The scoped
 * (closure) API, watchdog, keyspace-event recovery, cluster routing, the Functions
 * delivery path, and observability arrive in later milestones.
 */
export class RwLock {
  private readonly redis: RwlockRedis;
  private readonly cfg: Required<RwLockConfig>;
  private ready?: Promise<void>;
  private serverTimeOffsetMs = 0;

  constructor(redis: Redis, config: RwLockConfig = {}) {
    this.redis = redis as RwlockRedis;
    this.cfg = { ...DEFAULTS, ...config };
  }

  /** Define the scripts and learn the Redis clock offset once, lazily. */
  private ensureReady(): Promise<void> {
    if (!this.ready) {
      this.ready = (async () => {
        defineScripts(this.redis);
        const t = (await this.redis.time()) as unknown as [string, string];
        const serverMs = Number(t[0]) * 1000 + Math.floor(Number(t[1]) / 1000);
        this.serverTimeOffsetMs = serverMs - Date.now();
      })().catch((err) => {
        this.ready = undefined; // allow a later retry
        throw new BackendUnavailable("failed to initialise against Redis", { cause: err });
      });
    }
    return this.ready;
  }

  /** Best-effort estimate of the Redis server clock; used only to size timeouts. */
  private serverNow(): number {
    return Date.now() + this.serverTimeOffsetMs;
  }

  acquireWrite(resource: string, opts: AcquireOptions = {}): Promise<LockHandle> {
    return this.acquire(resource, "write", opts);
  }

  acquireRead(resource: string, opts: AcquireOptions = {}): Promise<LockHandle> {
    return this.acquire(resource, "read", opts);
  }

  async acquire(resource: string, mode: LockMode, opts: AcquireOptions = {}): Promise<LockHandle> {
    await this.ensureReady();
    const o = this.resolveOptions(opts);
    const prefix = keyPrefix(resource);
    const requestId = newRequestId();

    let res: unknown[];
    try {
      res = await this.redis.rwlockAcquire(
        prefix,
        mode,
        o.leaseMs,
        o.waitMs,
        requestId,
        o.ownerId,
        o.fairness,
        o.maxReaderBatch,
        this.cfg.notifyKeyTtlMs,
        this.cfg.requestKeyTtlGraceMs,
      );
    } catch (err) {
      throw new BackendUnavailable(`acquire failed for ${resource}`, { cause: err });
    }

    const status = String(res[0]);
    if (status === "GRANTED") {
      return {
        resource,
        mode: String(res[4]) as LockMode,
        token: String(res[1]),
        fencingToken: Number(res[2]),
        leaseUntilMs: Number(res[3]),
      };
    }

    // QUEUED -> block on the private mailbox.
    const notifyKey = String(res[2]);
    const waitDeadlineMs = Number(res[3]);
    const headHolderLeaseMs = Number(res[4]);
    return this.waitForGrant(resource, prefix, requestId, notifyKey, waitDeadlineMs, headHolderLeaseMs);
  }

  private async waitForGrant(
    resource: string,
    prefix: string,
    requestId: string,
    notifyKey: string,
    waitDeadlineMs: number,
    headHolderLeaseMs: number,
  ): Promise<LockHandle> {
    // A dedicated blocking connection so a BLPOP never ties up the user's pool
    // (Spec §15). M0 uses one connection per wait; pooling/backpressure is M6.
    const bconn = this.redis.duplicate();
    let holderLeaseMs = headHolderLeaseMs;
    try {
      for (;;) {
        const remaining = waitDeadlineMs - this.serverNow();
        if (remaining <= 0) {
          return await this.finishTimeout(resource, prefix, requestId, notifyKey);
        }

        // Block until the holder's lease boundary (to catch a crash) or the deadline.
        const boundary =
          holderLeaseMs > 0 ? holderLeaseMs - this.serverNow() + EPSILON_MS : remaining;
        const blockMs = Math.max(FLOOR_MS, Math.min(remaining, Math.max(boundary, 0) || remaining));

        let popped: [string, string] | null;
        try {
          popped = await bconn.blpop(notifyKey, blockMs / 1000);
        } catch (err) {
          throw new BackendUnavailable(`wait failed for ${resource}`, { cause: err });
        }
        if (popped) {
          return this.handleFromPayload(resource, popped[1]);
        }

        // Woke with no grant -> the holder may have crashed. Run maintenance once
        // (which may push a grant into our mailbox), refresh the boundary, re-block.
        try {
          await this.redis.rwlockExpireAndGrant(prefix, this.cfg.notifyKeyTtlMs);
        } catch (err) {
          throw new BackendUnavailable(`wait maintenance failed for ${resource}`, { cause: err });
        }
        holderLeaseMs = await this.peekHeadHolderLease(prefix);
      }
    } finally {
      bconn.disconnect();
    }
  }

  /** Last-instant reconciliation when the wait deadline elapses (Spec §8.4). */
  private async finishTimeout(
    resource: string,
    prefix: string,
    requestId: string,
    notifyKey: string,
  ): Promise<LockHandle> {
    let drained: string | null;
    try {
      drained = await this.redis.lpop(notifyKey); // were we granted at the buzzer?
    } catch (err) {
      throw new BackendUnavailable(`wait drain failed for ${resource}`, { cause: err });
    }
    if (drained) {
      return this.handleFromPayload(resource, drained);
    }
    try {
      await this.redis.rwlockCancelWait(prefix, requestId, this.cfg.notifyKeyTtlMs);
    } catch (err) {
      throw new BackendUnavailable(`cancel_wait failed for ${resource}`, { cause: err });
    }
    throw new WaitTimeout(`acquire timed out for ${resource}`);
  }

  private async peekHeadHolderLease(prefix: string): Promise<number> {
    try {
      const r = (await this.redis.zrange(`${prefix}:holders`, 0, 0, "WITHSCORES")) as string[];
      return r.length >= 2 ? Number(r[1]) : -1;
    } catch {
      return -1; // a transient read failure just means we re-block for the remaining time
    }
  }

  private handleFromPayload(resource: string, payload: string): LockHandle {
    const g = JSON.parse(payload) as GrantPayload;
    return {
      resource,
      mode: g.mode,
      token: g.token,
      fencingToken: g.fencing,
      leaseUntilMs: g.lease_until_ms,
    };
  }

  /** Release a held lock. NOT_HELD is a benign no-op (the lease may have expired). */
  async release(handle: LockHandle): Promise<void> {
    await this.ensureReady();
    try {
      await this.redis.rwlockRelease(keyPrefix(handle.resource), handle.token, this.cfg.notifyKeyTtlMs);
    } catch (err) {
      throw new BackendUnavailable(`release failed for ${handle.resource}`, { cause: err });
    }
  }

  /**
   * Renew a held lease. Throws LockLost if the lock is no longer ours.
   * The client-side safety-margin guard (Spec §9.2) lands with the watchdog in M3.
   */
  async extend(handle: LockHandle, leaseMs?: number): Promise<LockHandle> {
    await this.ensureReady();
    const lease = this.clampLease(leaseMs ?? this.cfg.defaultLeaseMs);
    let res: unknown[];
    try {
      res = await this.redis.rwlockExtend(keyPrefix(handle.resource), handle.token, lease);
    } catch (err) {
      throw new BackendUnavailable(`extend failed for ${handle.resource}`, { cause: err });
    }
    if (String(res[0]) === "LOST") {
      throw new LockLost(`lock lost for ${handle.resource}`);
    }
    return { ...handle, leaseUntilMs: Number(res[1]) };
  }

  private clampLease(leaseMs: number): number {
    return Math.max(1, Math.min(this.cfg.maxLeaseMs, Math.floor(leaseMs)));
  }

  private resolveOptions(opts: AcquireOptions): {
    leaseMs: number;
    waitMs: number;
    ownerId: string;
    fairness: Fairness;
    maxReaderBatch: number;
  } {
    const ownerId = opts.ownerId ?? "";
    if (this.cfg.requireOwnerId && ownerId === "") {
      throw new TypeError("ownerId is required (set requireOwnerId: false to opt out)");
    }
    const leaseMs = this.clampLease(opts.leaseMs ?? this.cfg.defaultLeaseMs);
    const waitMs = Math.max(0, Math.min(this.cfg.maxWaitMs, Math.floor(opts.waitMs ?? this.cfg.defaultWaitMs)));
    return {
      leaseMs,
      waitMs,
      ownerId,
      fairness: opts.fairness ?? this.cfg.defaultFairness,
      maxReaderBatch: opts.maxReaderBatch ?? this.cfg.maxReaderBatch,
    };
  }
}
