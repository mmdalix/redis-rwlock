import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { createClient } from "redis";
import { RwLock, WaitTimeoutError, BackendUnavailableError, LockLostError, type RwLockConfig } from "../src/index.js";
import { startRedis, type RedisHarness } from "./redis-harness.js";

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

async function mk(cfg?: RwLockConfig): Promise<RwLock> {
  const client = await harness.newClient();
  const rw = new RwLock(client, { requireOwnerId: true, ...cfg });
  locks.push(rw);
  return rw;
}

describe("M0 write lock", () => {
  it("grants an uncontended write lock with a fencing token and future lease", async () => {
    const rw = await mk();
    const before = Date.now();
    const h = await rw.acquireWrite("r", { ownerId: "w1", leaseMs: 30_000, waitMs: 1000 });
    expect(h.mode).toBe("write");
    expect(h.token).toContain("w1");
    expect(h.fencingToken).toBeGreaterThan(0);
    expect(h.leaseUntilMs).toBeGreaterThan(before);
    await rw.release(h);
  });

  it("is idempotent on double release (NOT_HELD is a no-op)", async () => {
    const rw = await mk();
    const h = await rw.acquireWrite("r", { ownerId: "w1" });
    await rw.release(h);
    await expect(rw.release(h)).resolves.toBeUndefined();
  });

  it("excludes a second writer and times out within waitMs", async () => {
    const rw = await mk();
    const h = await rw.acquireWrite("r", { ownerId: "w1", leaseMs: 30_000 });
    const start = Date.now();
    await expect(rw.acquireWrite("r", { ownerId: "w2", waitMs: 300 })).rejects.toBeInstanceOf(
      WaitTimeoutError,
    );
    const elapsed = Date.now() - start;
    expect(elapsed).toBeGreaterThanOrEqual(250);
    expect(elapsed).toBeLessThan(3000);
    await rw.release(h);
  });

  it("hands the lock to a waiting writer immediately on release (no polling)", async () => {
    const a = await mk();
    const b = await mk();
    const h1 = await a.acquireWrite("r", { ownerId: "w1", leaseMs: 30_000 });

    const start = Date.now();
    const waiter = b.acquireWrite("r", { ownerId: "w2", leaseMs: 30_000, waitMs: 5000 });
    // give the waiter a moment to enqueue and block on BLPOP
    await new Promise((r) => setTimeout(r, 150));
    await a.release(h1);

    const h2 = await waiter;
    const elapsed = Date.now() - start;
    expect(h2.token).toContain("w2");
    expect(h2.fencingToken).toBeGreaterThan(h1.fencingToken);
    // handoff should be fast (RTT-bound), well under the full wait
    expect(elapsed).toBeLessThan(2000);
    await b.release(h2);
  });

  it("issues strictly increasing fencing tokens per resource", async () => {
    const rw = await mk();
    const tokens: number[] = [];
    for (let i = 0; i < 5; i++) {
      const h = await rw.acquireWrite("r", { ownerId: `w${i}` });
      tokens.push(h.fencingToken);
      await rw.release(h);
    }
    for (let i = 1; i < tokens.length; i++) {
      expect(tokens[i]).toBeGreaterThan(tokens[i - 1]!);
    }
  });

  it("recovers a crashed holder via self-wake within the lease window", async () => {
    const a = await mk();
    const b = await mk();
    // A acquires a short lease and never releases (simulated crash).
    await a.acquireWrite("r", { ownerId: "w1", leaseMs: 400 });

    const start = Date.now();
    const h2 = await b.acquireWrite("r", { ownerId: "w2", leaseMs: 5000, waitMs: 8000 });
    const elapsed = Date.now() - start;

    expect(h2.token).toContain("w2");
    // should be granted shortly after the 400ms lease expires (self-wake + epsilon + RTT)
    expect(elapsed).toBeGreaterThanOrEqual(350);
    expect(elapsed).toBeLessThan(2500);
    await b.release(h2);
  });

  it("reclaims a crashed holder lazily for a later acquirer (no waiter present)", async () => {
    const a = await mk();
    const b = await mk();
    await a.acquireWrite("r", { ownerId: "w1", leaseMs: 200 });
    await new Promise((r) => setTimeout(r, 300)); // let the lease expire with nobody waiting
    const h = await b.acquireWrite("r", { ownerId: "w2", waitMs: 1000 });
    expect(h.token).toContain("w2");
    await b.release(h);
  });

  it("fails closed (BackendUnavailableError) when Redis is unreachable", async () => {
    const dead = createClient({ socket: { port: 6390, reconnectStrategy: false } });
    dead.on("error", () => {});
    await dead.connect().catch(() => {}); // connection refused
    const rw = new RwLock(dead, { requireOwnerId: false });
    await expect(rw.acquireWrite("r", { waitMs: 200 })).rejects.toBeInstanceOf(BackendUnavailableError);
    await rw.close().catch(() => {});
  });

  it("extend renews a live lock and returns LOST after expiry", async () => {
    const rw = await mk();
    const h = await rw.acquireWrite("r", { ownerId: "w1", leaseMs: 30_000 });
    const before = h.leaseUntilMs;
    const renewed = await rw.extend(h, 60_000);
    expect(renewed.leaseUntilMs).toBeGreaterThan(before);
    await rw.release(renewed);

    const h2 = await rw.acquireWrite("r", { ownerId: "w1", leaseMs: 200 });
    await new Promise((r) => setTimeout(r, 300));
    await expect(rw.extend(h2, 1000)).rejects.toBeInstanceOf(LockLostError);
  });
});
