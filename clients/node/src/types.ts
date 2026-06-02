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
  /** Use the keyspace-expiry subscriber if the server has it enabled (SPEC §10.3,
   *  §17). "auto" detects via CONFIG GET (never CONFIG SET); "off" disables it.
   *  Default "auto". Falls back silently to the per-waiter self-wake path. */
  keyspaceEvents?: "auto" | "off";
  /** Invoked after a recovery sweep triggered by a keyspace-expiry event. */
  onRecovery?: (resource: string) => void;
  /** Lua delivery: "functions" (FCALL), "scripts" (EVALSHA), or "auto" (Functions if
   *  available, else EVALSHA). Default "auto" (SPEC §17). */
  delivery?: "auto" | "functions" | "scripts";
  /** Contend even if the server's protocol MAJOR differs from this client (SPEC §16).
   *  Default false — a mismatch raises IncompatibleServerLogicError. */
  allowIncompatibleProtocol?: boolean;
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
  keyspaceEvents: "auto" as "auto" | "off",
  onRecovery: (_resource: string): void => {},
  delivery: "auto" as "auto" | "functions" | "scripts",
  allowIncompatibleProtocol: false,
} as const;
