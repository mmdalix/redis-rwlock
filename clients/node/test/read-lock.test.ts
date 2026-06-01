import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { RwLock, WaitTimeout } from "../src/index.js";
import { startRedis, type RedisHarness } from "./redis-harness.js";

// Read locks land fully in M1; these smoke tests assert the core invariants are
// already wired through grant_from_queue (readers co-hold; reader/writer exclude).

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

async function mk(): Promise<RwLock> {
  const client = await harness.newClient();
  const rw = new RwLock(client);
  locks.push(rw);
  return rw;
}

describe("read lock (M1 preview)", () => {
  it("lets multiple readers hold concurrently", async () => {
    const rw = await mk();
    const a = await rw.acquireRead("r", { ownerId: "r1", leaseMs: 30_000 });
    const b = await rw.acquireRead("r", { ownerId: "r2", leaseMs: 30_000 });
    expect(a.mode).toBe("read");
    expect(b.mode).toBe("read");
    expect(b.fencingToken).toBeGreaterThan(a.fencingToken);
    await rw.release(a);
    await rw.release(b);
  });

  it("blocks a reader while a writer holds, then grants it on release", async () => {
    const w = await mk();
    const r = await mk();
    const hw = await w.acquireWrite("r", { ownerId: "w1", leaseMs: 30_000 });
    const waiter = r.acquireRead("r", { ownerId: "r1", leaseMs: 30_000, waitMs: 5000 });
    await new Promise((res) => setTimeout(res, 150));
    await w.release(hw);
    const hr = await waiter;
    expect(hr.mode).toBe("read");
    await r.release(hr);
  });

  it("write_preferring: a queued writer blocks new readers from jumping ahead", async () => {
    const r1 = await mk();
    const wr = await mk();
    const r2 = await mk();
    // r1 holds a read lock
    const h1 = await r1.acquireRead("r", { ownerId: "r1", leaseMs: 30_000 });
    // a writer queues behind it
    const writerWait = wr.acquireWrite("r", { ownerId: "w1", leaseMs: 30_000, waitMs: 5000 });
    await new Promise((res) => setTimeout(res, 150));
    // a new reader must NOT jump ahead of the queued writer -> times out
    await expect(
      r2.acquireRead("r", { ownerId: "r2", leaseMs: 30_000, waitMs: 300 }),
    ).rejects.toBeInstanceOf(WaitTimeout);
    // once the holding reader releases, the writer gets it
    await r1.release(h1);
    const hw = await writerWait;
    expect(hw.mode).toBe("write");
    await wr.release(hw);
  });
});
