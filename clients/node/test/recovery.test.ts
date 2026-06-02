import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { RwLock, type RwLockConfig } from "../src/index.js";
import { startRedis, type RedisHarness } from "./redis-harness.js";

// M4: recovery. Lazy cleanup and per-waiter self-wake are exercised in write-lock.test.ts;
// here we focus on the optional, auto-detected keyspace-expiry subscriber (SPEC §10.3).

const settle = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function holderCount(h: RedisHarness, r: string): Promise<number> {
  const c = await h.newClient();
  const readers = Number(await c.zCard(`rwlock:{${r}}:readers`));
  const writer = Number(await c.exists(`rwlock:{${r}}:writer`));
  return readers + writer;
}

describe("M4 keyspace subscriber (events ENABLED)", () => {
  let harness: RedisHarness;
  const locks: RwLock[] = [];
  beforeAll(async () => {
    harness = await startRedis({ notifyKeyspaceEvents: "Ex" });
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
    const rw = new RwLock(await harness.newClient(), config);
    locks.push(rw);
    return rw;
  }

  it("detects the capability and activates the subscriber", async () => {
    const rw = await mk();
    await rw.acquireWrite("k", { ownerId: "w1", leaseMs: 30_000 }).then((h) => rw.release(h));
    expect(rw.keyspaceActive).toBe(true);
  });

  it("proactively reclaims a crashed holder with NO waiter (subscriber-driven)", async () => {
    const recovered: string[] = [];
    const rw = await mk({ onRecovery: (r) => recovered.push(r) });

    // crash: take a short lease and never release; nobody is waiting.
    await rw.acquireWrite("orphan", { ownerId: "w1", leaseMs: 300 });
    expect(await holderCount(harness, "orphan")).toBe(1);

    // the lease-expiry sentinel fires ~300ms later -> subscriber runs expire_and_grant,
    // sweeping the dead holder even though no other operation touched the resource.
    await settle(900);
    expect(await holderCount(harness, "orphan")).toBe(0);
    expect(recovered).toContain("orphan");
  });

  it("still recovers a waiting acquirer after a holder crash", async () => {
    const a = await mk();
    const b = await mk();
    await a.acquireWrite("h", { ownerId: "w1", leaseMs: 400 }); // crash (never released)
    const start = Date.now();
    const h2 = await b.acquireWrite("h", { ownerId: "w2", leaseMs: 5000, waitMs: 8000 });
    expect(h2.token).toContain("w2");
    expect(Date.now() - start).toBeLessThan(2500);
    await b.release(h2);
  });
});

describe("M4 keyspace subscriber (events DISABLED)", () => {
  let harness: RedisHarness;
  const locks: RwLock[] = [];
  beforeAll(async () => {
    harness = await startRedis(); // no notify-keyspace-events
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
    const rw = new RwLock(await harness.newClient(), config);
    locks.push(rw);
    return rw;
  }

  it("does not activate the subscriber (silent fallback)", async () => {
    const rw = await mk();
    await rw.acquireWrite("k", { ownerId: "w1" }).then((h) => rw.release(h));
    expect(rw.keyspaceActive).toBe(false);
  });

  it("a crashed reader lingers until the next op reclaims it via lazy cleanup", async () => {
    const rw = await mk();
    // A crashed READER lives in a ZSET with no native TTL, so without events or any
    // other op it lingers past its lease. (A crashed WRITER, by contrast, self-expires
    // via its key TTL — exercised by the events-enabled suite.)
    await rw.acquireRead("orphan", { ownerId: "r1", leaseMs: 300 });
    await settle(900);
    expect(await holderCount(harness, "orphan")).toBe(1);
    // the next acquirer sweeps the expired reader and is granted
    const h = await rw.acquireWrite("orphan", { ownerId: "w2", waitMs: 1000 });
    expect(h.token).toContain("w2");
    await rw.release(h);
  });

  it("recovers from a crashed queued writer (no phantom queued_writers starvation)", async () => {
    const rw = await mk();
    const admin = await harness.newClient();
    const p = "rwlock:{phantom}";
    // Simulate the post-crash state: a queued writer whose req hash has TTL-expired,
    // leaving an orphan queue entry. (queued_writers is derived from live req hashes,
    // so there is no separate counter to inflate — the orphan itself is the hazard.)
    await admin.zAdd(`${p}:queue`, { score: 1, value: "ghost-writer-req" });

    // prune_queue must drop the orphan and derive queued_writers=0, so the reader is
    // granted immediately instead of being wedged behind a phantom writer.
    const h = await rw.acquireRead("phantom", { ownerId: "r1", leaseMs: 30_000, waitMs: 1000 });
    expect(h.mode).toBe("read");
    expect((await rw.inspect("phantom")).queuedWriters).toBe(0);
    await rw.release(h);
  });

  it("respects keyspaceEvents:'off' even when the server supports them", async () => {
    const rw = await mk({ keyspaceEvents: "off" });
    await rw.acquireWrite("k", { ownerId: "w1" }).then((h) => rw.release(h));
    expect(rw.keyspaceActive).toBe(false);
  });
});
