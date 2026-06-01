import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { RwLock, WaitTimeout, BackendUnavailable, LockLost } from "../src/index.js";
import { startRedis, type RedisHarness } from "./redis-harness.js";

let harness: RedisHarness;

beforeAll(async () => {
  harness = await startRedis();
});
afterAll(async () => {
  await harness.stop();
});
beforeEach(async () => {
  await harness.flush();
});

function lock(): { rw: RwLock; redis: Redis } {
  const redis = harness.client();
  return { rw: new RwLock(redis, { requireOwnerId: true }), redis };
}

describe("M0 write lock", () => {
  it("grants an uncontended write lock with a fencing token and future lease", async () => {
    const { rw } = lock();
    const before = Date.now();
    const h = await rw.acquireWrite("r", { ownerId: "w1", leaseMs: 30_000, waitMs: 1000 });
    expect(h.mode).toBe("write");
    expect(h.token).toContain("w1");
    expect(h.fencingToken).toBeGreaterThan(0);
    expect(h.leaseUntilMs).toBeGreaterThan(before);
    await rw.release(h);
  });

  it("is idempotent on double release (NOT_HELD is a no-op)", async () => {
    const { rw } = lock();
    const h = await rw.acquireWrite("r", { ownerId: "w1" });
    await rw.release(h);
    await expect(rw.release(h)).resolves.toBeUndefined();
  });

  it("excludes a second writer and times out within waitMs", async () => {
    const { rw } = lock();
    const h = await rw.acquireWrite("r", { ownerId: "w1", leaseMs: 30_000 });
    const start = Date.now();
    await expect(rw.acquireWrite("r", { ownerId: "w2", waitMs: 300 })).rejects.toBeInstanceOf(
      WaitTimeout,
    );
    const elapsed = Date.now() - start;
    expect(elapsed).toBeGreaterThanOrEqual(250);
    expect(elapsed).toBeLessThan(3000);
    await rw.release(h);
  });

  it("hands the lock to a waiting writer immediately on release (no polling)", async () => {
    const a = lock();
    const b = lock();
    const h1 = await a.rw.acquireWrite("r", { ownerId: "w1", leaseMs: 30_000 });

    const start = Date.now();
    const waiter = b.rw.acquireWrite("r", { ownerId: "w2", leaseMs: 30_000, waitMs: 5000 });
    // give the waiter a moment to enqueue and block on BLPOP
    await new Promise((r) => setTimeout(r, 150));
    await a.rw.release(h1);

    const h2 = await waiter;
    const elapsed = Date.now() - start;
    expect(h2.token).toContain("w2");
    expect(h2.fencingToken).toBeGreaterThan(h1.fencingToken);
    // handoff should be fast (RTT-bound), well under the full wait
    expect(elapsed).toBeLessThan(2000);
    await b.rw.release(h2);
  });

  it("issues strictly increasing fencing tokens per resource", async () => {
    const { rw } = lock();
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
    const a = lock();
    const b = lock();
    // A acquires a short lease and never releases (simulated crash).
    await a.rw.acquireWrite("r", { ownerId: "w1", leaseMs: 400 });

    const start = Date.now();
    const h2 = await b.rw.acquireWrite("r", { ownerId: "w2", leaseMs: 5000, waitMs: 8000 });
    const elapsed = Date.now() - start;

    expect(h2.token).toContain("w2");
    // should be granted shortly after the 400ms lease expires (self-wake + epsilon + RTT)
    expect(elapsed).toBeGreaterThanOrEqual(350);
    expect(elapsed).toBeLessThan(2500);
    await b.rw.release(h2);
  });

  it("reclaims a crashed holder lazily for a later acquirer (no waiter present)", async () => {
    const a = lock();
    const b = lock();
    await a.rw.acquireWrite("r", { ownerId: "w1", leaseMs: 200 });
    await new Promise((r) => setTimeout(r, 300)); // let the lease expire with nobody waiting
    const h = await b.rw.acquireWrite("r", { ownerId: "w2", waitMs: 1000 });
    expect(h.token).toContain("w2");
    await b.rw.release(h);
  });

  it("fails closed (BackendUnavailable) when Redis is unreachable", async () => {
    const redis = new Redis({ port: 6390, lazyConnect: true, maxRetriesPerRequest: 1, retryStrategy: () => null });
    redis.on("error", () => {});
    const rw = new RwLock(redis, { requireOwnerId: false });
    await expect(rw.acquireWrite("r", { waitMs: 200 })).rejects.toBeInstanceOf(BackendUnavailable);
    redis.disconnect();
  });

  it("extend renews a live lock and returns LOST after expiry", async () => {
    const { rw } = lock();
    const h = await rw.acquireWrite("r", { ownerId: "w1", leaseMs: 30_000 });
    const renewed = await rw.extend(h, 60_000);
    expect(renewed.leaseUntilMs).toBeGreaterThan(h.leaseUntilMs);
    await rw.release(renewed);

    const h2 = await rw.acquireWrite("r", { ownerId: "w1", leaseMs: 200 });
    await new Promise((r) => setTimeout(r, 300));
    await expect(rw.extend(h2, 1000)).rejects.toBeInstanceOf(LockLost);
  });
});
