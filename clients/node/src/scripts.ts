// Registers the shared Lua as ioredis custom commands. ioredis transparently uses
// EVALSHA and falls back to EVAL on NOSCRIPT, which covers M0's delivery path; the
// FUNCTION-library path and version handshake arrive in a later milestone.

import type { Redis } from "ioredis";
import { SCRIPTS } from "./lua.generated.js";

// Custom commands attached to the ioredis client. Not part of ioredis's types, so
// callers reach them through the RwlockRedis cast below.
export interface RwlockCommands {
  rwlockAcquire(
    prefix: string,
    mode: string,
    leaseMs: number,
    waitMs: number,
    requestId: string,
    ownerId: string,
    fairness: string,
    maxReaderBatch: number,
    notifyKeyTtlMs: number,
    requestKeyTtlGraceMs: number,
  ): Promise<unknown[]>;
  rwlockRelease(prefix: string, token: string, notifyKeyTtlMs: number): Promise<unknown[]>;
  rwlockExtend(prefix: string, token: string, leaseMs: number): Promise<unknown[]>;
  rwlockCancelWait(prefix: string, requestId: string, notifyKeyTtlMs: number): Promise<unknown[]>;
  rwlockExpireAndGrant(prefix: string, notifyKeyTtlMs: number): Promise<unknown[]>;
}

export type RwlockRedis = Redis & RwlockCommands;

export function defineScripts(redis: Redis): void {
  const r = redis as Redis & { rwlockAcquire?: unknown };
  if (r.rwlockAcquire) return; // already defined on this connection
  redis.defineCommand("rwlockAcquire", { numberOfKeys: 1, lua: SCRIPTS.acquire });
  redis.defineCommand("rwlockRelease", { numberOfKeys: 1, lua: SCRIPTS.release });
  redis.defineCommand("rwlockExtend", { numberOfKeys: 1, lua: SCRIPTS.extend });
  redis.defineCommand("rwlockCancelWait", { numberOfKeys: 1, lua: SCRIPTS.cancelWait });
  redis.defineCommand("rwlockExpireAndGrant", { numberOfKeys: 1, lua: SCRIPTS.expireAndGrant });
}
