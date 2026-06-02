import { LockLostError } from "./errors.js";
import type { LockMode } from "./types.js";

/** The script-level primitives a handle needs from its owning RwLock. */
export interface HandleOwner {
  releaseToken(resource: string, token: string): Promise<void>;
  /** Returns the new lease_until_ms, or throws LockLostError if the token is gone. */
  extendToken(resource: string, token: string, leaseMs: number): Promise<number>;
  serverNow(): number;
  readonly extensionMarginMs: number;
  readonly defaultLeaseMs: number;
}

export interface LockHandleData {
  resource: string;
  mode: LockMode;
  token: string;
  fencingToken: number;
  leaseUntilMs: number;
}

/**
 * A held lock. Carries the fencing token (enforce it at your resource!) and an
 * `AbortSignal` that fires the instant the lock is lost. Implements AsyncDisposable
 * so `await using lock = await rw.acquireWrite(...)` releases at scope end.
 */
export class LockHandle implements AsyncDisposable {
  readonly resource: string;
  readonly mode: LockMode;
  readonly token: string;
  readonly fencingToken: number;
  leaseUntilMs: number;

  private readonly owner: HandleOwner;
  private readonly ac = new AbortController();
  private released = false;
  private settled = false;
  private watchdogTimer?: ReturnType<typeof setInterval>;
  private readonly onSettle: (result: "released" | "lost") => void;

  constructor(
    owner: HandleOwner,
    data: LockHandleData,
    onSettle: (result: "released" | "lost") => void = () => {},
  ) {
    this.owner = owner;
    this.resource = data.resource;
    this.mode = data.mode;
    this.token = data.token;
    this.fencingToken = data.fencingToken;
    this.leaseUntilMs = data.leaseUntilMs;
    this.onSettle = onSettle;
  }

  private settle(result: "released" | "lost"): void {
    if (this.settled) return;
    this.settled = true;
    this.onSettle(result);
  }

  /** Aborts the instant the lock is lost (its `reason` is a LockLostError). Stays
   *  un-aborted on a normal release — releasing is intentional, not a loss. */
  get signal(): AbortSignal {
    return this.ac.signal;
  }

  /**
   * Renew the lease. Fails closed (LockLostError) both when the server reports the
   * token is gone and — per the safety margin (SPEC §9.2) — when we are already so
   * close to expiry that a renewal might land after the lock was reclaimed.
   */
  async extend(leaseMs?: number): Promise<this> {
    if (this.released) throw new LockLostError(`lock for ${this.resource} already released`);
    if (this.owner.serverNow() >= this.leaseUntilMs - this.owner.extensionMarginMs) {
      const err = new LockLostError(`lock for ${this.resource} is within the extension safety margin`);
      this.loseLock(err);
      throw err;
    }
    const ms = leaseMs ?? this.owner.defaultLeaseMs;
    try {
      this.leaseUntilMs = await this.owner.extendToken(this.resource, this.token, ms);
    } catch (err) {
      if (err instanceof LockLostError) this.loseLock(err);
      throw err;
    }
    return this;
  }

  /** Release the lock (idempotent). Stops the watchdog. */
  async release(): Promise<void> {
    if (this.released) return;
    this.released = true;
    this.stopWatchdog();
    await this.owner.releaseToken(this.resource, this.token);
    this.settle("released");
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.release();
  }

  /** @internal Start the auto-extend watchdog (SPEC §9.3); refreshes at ~lease/3. */
  startWatchdog(leaseMs: number): void {
    const interval = Math.max(100, Math.floor(leaseMs / 3));
    this.watchdogTimer = setInterval(() => void this.watchdogTick(leaseMs), interval);
    this.watchdogTimer.unref?.();
  }

  private async watchdogTick(leaseMs: number): Promise<void> {
    if (this.released || this.ac.signal.aborted) return;
    try {
      await this.extend(leaseMs);
    } catch {
      // extend() has already aborted the signal on a real loss; transient backend
      // errors are retried on the next tick (the lease is the ultimate backstop).
    }
  }

  private stopWatchdog(): void {
    if (this.watchdogTimer !== undefined) {
      clearInterval(this.watchdogTimer);
      this.watchdogTimer = undefined;
    }
  }

  private loseLock(err: LockLostError): void {
    this.released = true;
    this.stopWatchdog();
    if (!this.ac.signal.aborted) this.ac.abort(err);
    this.settle("lost");
  }
}
