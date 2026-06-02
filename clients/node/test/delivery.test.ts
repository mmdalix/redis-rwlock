import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
  RwLock,
  WaitTimeoutError,
  IncompatibleServerLogicError,
  PROTOCOL_VERSION,
  MODULE_KEY,
  type RwLockConfig,
} from "../src/index.js";
import { startRedis, type RedisHarness } from "./redis-harness.js";

// M5: Functions-or-EVALSHA delivery, capability probe, and the cross-version
// handshake (rwlock:__module__). Cluster hash-tag co-location is covered separately.

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

describe("M5 delivery selection & handshake", () => {
  it("auto-selects Functions (FCALL) on a server that supports FUNCTION LOAD", async () => {
    const rw = await mk();
    await rw.acquireWrite("d", { ownerId: "w1" }).then((h) => rw.release(h));
    expect(rw.deliveryMode).toBe("functions");
  });

  it("writes the module marker with the client's protocol version and sha", async () => {
    const rw = await mk();
    await rw.acquireWrite("d", { ownerId: "w1" }).then((h) => rw.release(h));

    const admin = await harness.newClient();
    const marker = (await admin.hGetAll(MODULE_KEY)) as Record<string, string>;
    expect(Number(marker.protocol_version)).toBe(PROTOCOL_VERSION);
    expect(marker.sha).toMatch(/^[0-9a-f]{40}$/);
    expect(marker.impl_version).toBeTruthy();
    expect(rw.module?.protocolVersion).toBe(PROTOCOL_VERSION);
  });

  it("refuses to contend against an incompatible protocol major", async () => {
    const admin = await harness.newClient();
    await admin.hSet(MODULE_KEY, {
      protocol_version: "999",
      impl_version: "9.9.9",
      sha: "deadbeef",
      loaded_at_ms: String(Date.now()),
    });
    const rw = await mk();
    await expect(rw.acquireWrite("d", { ownerId: "w1" })).rejects.toBeInstanceOf(
      IncompatibleServerLogicError,
    );
  });

  it("contends across an incompatible major when explicitly allowed", async () => {
    const admin = await harness.newClient();
    await admin.hSet(MODULE_KEY, { protocol_version: "999", impl_version: "9", sha: "x", loaded_at_ms: "0" });
    const rw = await mk({ allowIncompatibleProtocol: true });
    const h = await rw.acquireWrite("d", { ownerId: "w1" });
    expect(h.mode).toBe("write");
    await rw.release(h);
  });
});

// The same observable behaviour must hold whether logic runs via FCALL or EVALSHA.
describe.each(["functions", "scripts"] as const)("M5 delivery parity — %s", (delivery) => {
  it("selects the requested delivery mode", async () => {
    const rw = await mk({ delivery });
    await rw.acquireWrite("p", { ownerId: "w1" }).then((h) => rw.release(h));
    expect(rw.deliveryMode).toBe(delivery);
  });

  it("write mutual exclusion + immediate hand-off on release", async () => {
    const a = await mk({ delivery });
    const b = await mk({ delivery });
    const h1 = await a.acquireWrite("p2", { ownerId: "w1", leaseMs: 30_000 });
    const waiter = b.acquireWrite("p2", { ownerId: "w2", leaseMs: 30_000, waitMs: 5000 });
    await settle(120);
    await a.release(h1);
    const h2 = await waiter;
    expect(h2.fencingToken).toBeGreaterThan(h1.fencingToken);
    await b.release(h2);
  });

  it("readers co-hold and a writer is excluded", async () => {
    const rw = await mk({ delivery });
    const r1 = await rw.acquireRead("p3", { ownerId: "r1", leaseMs: 30_000 });
    const r2 = await rw.acquireRead("p3", { ownerId: "r2", leaseMs: 30_000 });
    expect(r2.fencingToken).toBeGreaterThan(r1.fencingToken);
    await expect(
      rw.acquireWrite("p3", { ownerId: "w1", waitMs: 200 }),
    ).rejects.toBeInstanceOf(WaitTimeoutError);
    await rw.release(r1);
    await rw.release(r2);
  });

  it("extend renews the lease", async () => {
    const rw = await mk({ delivery });
    const h = await rw.acquireWrite("p4", { ownerId: "w1", leaseMs: 30_000 });
    const before = h.leaseUntilMs;
    const renewed = await rw.extend(h, 60_000);
    expect(renewed.leaseUntilMs).toBeGreaterThan(before);
    await rw.release(renewed);
  });
});
