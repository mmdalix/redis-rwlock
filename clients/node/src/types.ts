// Public, language-neutral contract (Spec §13.1) and defaults (Spec §19).

export type LockMode = "read" | "write";

export type Fairness = "write_preferring" | "fifo" | "read_preferring";

export interface AcquireOptions {
  /** Requested hold duration. Default 30000, clamped to maxLeaseMs. */
  leaseMs?: number;
  /** How long to block waiting. Default 10000, clamped to maxWaitMs. */
  waitMs?: number;
  /** Caller-chosen identity for "who" holds it. Required by default. */
  ownerId?: string;
  /** Fairness policy. Default write_preferring (matches Go sync.RWMutex). */
  fairness?: Fairness;
  /** Cap on readers woken by a single grant. Default 1000. */
  maxReaderBatch?: number;
  /** Auto-extend the lease while held (refreshes at ~lease/3). Default false (SPEC §9.3). */
  watchdog?: boolean;
  /** Cancel a *pending* acquire. The returned promise rejects with the signal's reason. */
  signal?: AbortSignal;
}

/** Tunables for the RwLock instance. All optional; sensible defaults per Spec §19. */
export interface RwLockConfig {
  defaultLeaseMs?: number;
  maxLeaseMs?: number;
  defaultWaitMs?: number;
  maxWaitMs?: number;
  defaultFairness?: Fairness;
  maxReaderBatch?: number;
  notifyKeyTtlMs?: number;
  requestKeyTtlGraceMs?: number;
  requireOwnerId?: boolean;
  /** Max dedicated connections for blocking BLPOP waits (SPEC §15). Default 16. */
  blockingPoolSize?: number;
  /** Refuse to extend within this margin of expiry (SPEC §9.2). Default 500ms. */
  extensionMarginMs?: number;
}

export const DEFAULTS = {
  defaultLeaseMs: 30_000,
  maxLeaseMs: 300_000,
  defaultWaitMs: 10_000,
  maxWaitMs: 60_000,
  defaultFairness: "write_preferring" as Fairness,
  maxReaderBatch: 1000,
  notifyKeyTtlMs: 60_000,
  requestKeyTtlGraceMs: 60_000,
  requireOwnerId: true,
  blockingPoolSize: 16,
  extensionMarginMs: 500,
} as const;
