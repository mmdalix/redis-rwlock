import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { RwLock, RwLockError, type RwLockConfig } from "../src/index.js";
import { startRedis, type RedisHarness } from "./redis-harness.js";

// Resource-safety review follow-ups: close() must guard entry points (typed error,
// not a raw TypeError) and be idempotent.

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
  const rw = new RwLock(await harness.newClient(), config);
  locks.push(rw);
  return rw;
}

describe("RwLock lifecycle", () => {
  it("acquires without an explicit ownerId (auto-defaults to <hostname>#<pid>)", async () => {
    const rw = await mk(); // default config -> requireOwnerId false
    const h = await rw.acquireWrite("auto-owner");
    expect(h.token).toContain("#"); // default owner separator
    await rw.release(h);
  });

  it("requireOwnerId:true still requires an explicit ownerId", async () => {
    const rw = await mk({ requireOwnerId: true });
    await expect(rw.acquireWrite("needs-owner")).rejects.toBeInstanceOf(TypeError);
  });

  it("rejects acquire after close() with a typed RwLockError (not a TypeError)", async () => {
    const rw = await mk();
    await rw.acquireWrite("lc", { ownerId: "w1" }).then((h) => rw.release(h));
    await rw.close();

    const err = await rw.acquireWrite("lc", { ownerId: "w2" }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(RwLockError);
    expect(err).not.toBeInstanceOf(TypeError);
  });

  it("is idempotent: close() twice resolves cleanly", async () => {
    const rw = await mk();
    await rw.acquireWrite("lc2", { ownerId: "w1" }).then((h) => rw.release(h));
    await rw.close();
    await expect(rw.close()).resolves.toBeUndefined();
  });

  it("close() racing first-time init settles cleanly and leaves the lock closed", async () => {
    const rw = new RwLock(await harness.newClient());
    locks.push(rw);
    // close() races the in-flight ensureReady() kicked off by the first acquire; it must
    // not hang or throw a raw TypeError, and the instance must end up closed.
    const acq = rw.acquireWrite("init-race", { ownerId: "w1", waitMs: 500 }).catch((e: unknown) => e);
    await rw.close();
    await acq; // must settle, not hang
    await expect(rw.acquireWrite("init-race", { ownerId: "w2" })).rejects.toBeInstanceOf(RwLockError);
  });

  it("recovers after an init failure (ready is cleared, no wedged state)", async () => {
    // A client pointed at a dead port: first acquire fails closed, and because
    // ensureReady tears down partial state and clears `ready`, the instance is reusable.
    const { createClient } = await import("redis");
    const dead = createClient({ socket: { port: 6391, reconnectStrategy: false } });
    dead.on("error", () => {});
    await dead.connect().catch(() => {});
    const rw = new RwLock(dead, { requireOwnerId: false });
    await expect(rw.acquireWrite("lc3", { waitMs: 100 })).rejects.toBeInstanceOf(RwLockError);
    await rw.close();
  });
});
