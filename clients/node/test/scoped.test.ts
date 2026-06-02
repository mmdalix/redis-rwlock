import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { RwLock, WaitTimeoutError, LockLostError, type RwLockConfig } from "../src/index.js";
import { startRedis, type RedisHarness } from "./redis-harness.js";

// M3: the scoped front door (withWriteLock/withReadLock), the opt-in watchdog,
// AbortSignal-on-loss, `await using`, and acquire cancellation via signal.

let harness: RedisHarness;
const locks: RwLock[] = [];

beforeAll(async () => {
  harness = await startRedis();
});
afterAll(async () => {
  await harness.stop();
});
beforeEach(async () => {
  await harness.flush();
});
afterEach(async () => {
  await Promise.all(locks.splice(0).map((l) => l.close().catch(() => {})));
});

async function mk(config?: RwLockConfig): Promise<RwLock> {
  const client = await harness.newClient();
  const rw = new RwLock(client, config);
  locks.push(rw);
  return rw;
}

const settle = (ms = 150) => new Promise((r) => setTimeout(r, ms));

async function holderCount(r: string): Promise<number> {
  const client = await harness.newClient();
  const readers = Number(await client.zCard(`rwlock:{${r}}:readers`));
  const writer = Number(await client.exists(`rwlock:{${r}}:writer`));
  return readers + writer;
}

describe("M3 scoped API", () => {
  it("runs the callback holding the lock and releases afterwards", async () => {
    const rw = await mk();
    let fencingSeen = 0;
    const result = await rw.withWriteLock("s", { ownerId: "w1" }, async (lock) => {
      expect(lock.mode).toBe("write");
      expect(await holderCount("s")).toBe(1);
      fencingSeen = lock.fencingToken;
      return "value";
    });
    expect(result).toBe("value");
    expect(fencingSeen).toBeGreaterThan(0);
    expect(await holderCount("s")).toBe(0); // released
  });

  it("releases even when the callback throws, and propagates the error", async () => {
    const rw = await mk();
    await expect(
      rw.withWriteLock("s", { ownerId: "w1" }, async () => {
        throw new Error("boom");
      }),
    ).rejects.toThrow("boom");
    expect(await holderCount("s")).toBe(0);
  });

  it("supports the (resource, fn) overload with defaults", async () => {
    const rw = await mk({ requireOwnerId: false });
    const out = await rw.withReadLock("s2", async (lock) => lock.mode);
    expect(out).toBe("read");
    expect(await holderCount("s2")).toBe(0);
  });

  it("excludes a competitor for the duration of the scope", async () => {
    const a = await mk();
    const b = await mk();
    const scope = a.withWriteLock("s3", { ownerId: "w1", leaseMs: 30_000 }, async () => {
      await settle(400);
      return "done";
    });
    await settle(80);
    await expect(
      b.acquireWrite("s3", { ownerId: "w2", waitMs: 150 }),
    ).rejects.toBeInstanceOf(WaitTimeoutError);
    expect(await scope).toBe("done");
  });
});

describe("M3 watchdog", () => {
  it("keeps a lock alive across multiple lease periods", async () => {
    // margin < lease/3 so the watchdog can safely re-extend a short lease.
    const a = await mk({ extensionMarginMs: 100 });
    const b = await mk();
    // lease 400ms, work ~1200ms -> only survives if the watchdog re-extends (3x).
    const scope = a.withWriteLock("wd", { ownerId: "w1", leaseMs: 400, watchdog: true }, async (lock) => {
      await settle(1200);
      expect(lock.signal.aborted).toBe(false); // never lost
      return "ok";
    });
    await settle(250);
    // a competitor cannot get in while the watchdog holds the lease
    await expect(
      b.acquireWrite("wd", { ownerId: "w2", waitMs: 700 }),
    ).rejects.toBeInstanceOf(WaitTimeoutError);
    expect(await scope).toBe("ok");
    expect(await holderCount("wd")).toBe(0);
  });

  it("aborts the lock signal when the lease is lost mid-scope", async () => {
    const rw = await mk({ extensionMarginMs: 100 });
    const admin = await harness.newClient();

    let abortedReason: unknown;
    await rw.withWriteLock("wd2", { ownerId: "w1", leaseMs: 600, watchdog: true }, async (lock) => {
      lock.signal.addEventListener("abort", () => {
        abortedReason = lock.signal.reason;
      });
      // simulate loss: forcibly evict this holder out from under us
      await admin.del(`rwlock:{wd2}:writer`);
      // wait for the next watchdog tick (~lease/3) to observe the loss
      await settle(500);
      expect(lock.signal.aborted).toBe(true);
    });
    expect(abortedReason).toBeInstanceOf(LockLostError);
  });
});

describe("M3 await using", () => {
  it("auto-releases the handle at the end of the block", async () => {
    const rw = await mk();
    {
      await using lock = await rw.acquireWrite("u", { ownerId: "w1" });
      expect(lock.mode).toBe("write");
      expect(await holderCount("u")).toBe(1);
    }
    expect(await holderCount("u")).toBe(0); // disposed -> released
  });
});

describe("M3 acquire cancellation", () => {
  it("rejects a pending acquire when its signal aborts", async () => {
    const a = await mk();
    const b = await mk();
    await a.acquireWrite("c", { ownerId: "w1", leaseMs: 30_000 }); // hold it

    const ac = new AbortController();
    const pending = b.acquireWrite("c", { ownerId: "w2", waitMs: 30_000, signal: ac.signal });
    await settle(120);
    ac.abort(new Error("caller cancelled"));

    await expect(pending).rejects.toThrow("caller cancelled");
    // the cancelled waiter left nothing behind
    expect(await holderCount("c")).toBe(1); // only the original holder
  });

  it("rejects immediately if the signal is already aborted", async () => {
    const rw = await mk();
    const pre = AbortSignal.abort(new Error("already"));
    await expect(rw.acquireWrite("c2", { ownerId: "w1", signal: pre })).rejects.toThrow("already");
  });
});
