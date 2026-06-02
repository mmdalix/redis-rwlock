import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { RwLock, type RwLockConfig } from "../src/index.js";
import { startRedis, type RedisHarness } from "./redis-harness.js";

// Regression tests for the v2 redesign / review findings:
//   F3 — a reader behind a crashed queued writer self-wakes at the writer's deadline.
//   F6 — extend never shortens the lease.
//   v2 — a crashed writer self-expires via its key TTL (no library action needed).

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

const settle = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function holders(r: string): Promise<number> {
  const c = await harness.newClient();
  const readers = Number(await c.zCard(`rwlock:{${r}}:readers`));
  const writer = Number(await c.exists(`rwlock:{${r}}:writer`));
  return readers + writer;
}

describe("v2 regressions", () => {
  it("F6: extend never moves the expiry earlier", async () => {
    const rw = await mk();
    const h = await rw.acquireWrite("f6", { ownerId: "w1", leaseMs: 30_000 });
    const before = h.leaseUntilMs;
    // ask for a much SHORTER lease — must not shorten the hold
    const renewed = await rw.extend(h, 500);
    expect(renewed.leaseUntilMs).toBeGreaterThanOrEqual(before);
    await rw.release(h);
  });

  it("v2: a crashed writer self-expires via its key TTL with no library action", async () => {
    const rw = await mk(); // events disabled on this harness; no subscriber, no other ops
    await rw.acquireWrite("selfexp", { ownerId: "w1", leaseMs: 300 });
    expect(await holders("selfexp")).toBe(1);
    await settle(900); // Redis natively expires the writer key
    expect(await holders("selfexp")).toBe(0);
  });

  it("F3: a reader behind a crashed queued writer is granted at the writer's deadline", async () => {
    const admin = await harness.newClient();
    const p = "rwlock:{f3}";
    const ghostDeadline = Date.now() + 400; // server clock ≈ local on a local redis
    // seq=1 so the ghost (score 1) sorts strictly before the reader (which INCRs to 2).
    await admin.set(`${p}:seq`, "1");
    // Inject a queued writer whose client has vanished (it will never cancel): its req
    // hash exists with a near deadline, and it sits at the head of the queue, no holders.
    await admin.hSet(`${p}:req:wghost`, {
      mode: "write",
      owner_id: "ghost",
      lease_ms: "30000",
      wait_deadline_ms: String(ghostDeadline),
      notify_key: `${p}:notify:wghost`,
      granted_token: "",
      created_at_ms: String(Date.now()),
      fairness: "write_preferring",
      max_reader_batch: "1000",
    });
    await admin.zAdd(`${p}:queue`, { score: 1, value: "wghost" });

    // A reader arrives: write_preferring sees a queued writer, so it queues behind it
    // with next_wake = the ghost's deadline. It must self-wake there, prune the dead
    // writer, and be granted — NOT block for its full waitMs.
    const reader = await mk();
    const start = Date.now();
    const h = await reader.acquireRead("f3", { ownerId: "r1", leaseMs: 30_000, waitMs: 5000 });
    const elapsed = Date.now() - start;
    expect(h.mode).toBe("read");
    expect(elapsed).toBeGreaterThanOrEqual(350); // after the ghost's deadline
    expect(elapsed).toBeLessThan(2000); // NOT the full 5s waitMs (the F3 bug)
    await reader.release(h);
  });
});
