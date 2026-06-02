import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { RwLock, WaitTimeoutError } from "../src/index.js";
import { startRedis, type RedisHarness } from "./redis-harness.js";

// M2: the genuine fairness-policy distinctions (read_preferring / fifo /
// write_preferring), enforced entirely inside the Lua, plus the ghost-grant
// timeout↔grant race (SPEC §20.5) proven under stress.

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

const settle = (ms = 200) => new Promise((r) => setTimeout(r, ms));

function track<T>(p: Promise<T>): { done: boolean; value?: T; err?: unknown } {
  const s: { done: boolean; value?: T; err?: unknown } = { done: false };
  p.then(
    (v) => {
      s.done = true;
      s.value = v;
    },
    (e) => {
      s.done = true;
      s.err = e;
    },
  );
  return s;
}

async function stateOf(r: string): Promise<{ mode: string; holders: number }> {
  const client = await harness.newClient();
  const readers = Number(await client.zCard(`rwlock:{${r}}:readers`));
  const writer = Number(await client.exists(`rwlock:{${r}}:writer`));
  const mode = writer ? "write" : readers > 0 ? "read" : "none";
  return { mode, holders: readers + writer };
}

describe("M2 fairness — read_preferring", () => {
  it("lets queued readers jump ahead of a queued writer (writer may starve)", async () => {
    const opt = { fairness: "read_preferring" as const, leaseMs: 30_000, waitMs: 8000 };
    const [w0, r1, w1, r2] = await Promise.all([mk(), mk(), mk(), mk()]);
    const hw0 = await w0.acquireWrite("rp", { ownerId: "W0", ...opt });

    // queue order behind the holder: R1, W1, R2
    const a1 = track(r1!.acquireRead("rp", { ownerId: "R1", ...opt }));
    await settle(40);
    const b1 = track(w1!.acquireWrite("rp", { ownerId: "W1", ...opt }));
    await settle(40);
    const a2 = track(r2!.acquireRead("rp", { ownerId: "R2", ...opt }));
    await settle(120);

    await w0.release(hw0);
    await settle(300);

    // R1 and R2 both granted (R2 jumped the queued writer W1); W1 starves.
    expect(a1.done && a1.value?.mode === "read").toBe(true);
    expect(a2.done && a2.value?.mode === "read").toBe(true);
    expect(b1.done).toBe(false);

    // once both readers release, the writer finally gets it.
    await r1!.release(a1.value!);
    await r2!.release(a2.value!);
    await settle(300);
    expect(b1.done && b1.value?.mode === "write").toBe(true);
    await w1!.release(b1.value!);
  });

  it("grants a new reader immediately even while a writer is queued", async () => {
    const opt = { fairness: "read_preferring" as const, leaseMs: 30_000 };
    const r0 = await mk();
    const wr = await mk();
    const r2 = await mk();

    const h0 = await r0.acquireRead("rp2", { ownerId: "R0", ...opt });
    const writer = track(wr.acquireWrite("rp2", { ownerId: "W1", ...opt, waitMs: 8000 }));
    await settle(150);

    // under read_preferring this is granted right away (no writer *holds*)
    const h2 = await r2.acquireRead("rp2", { ownerId: "R2", ...opt, waitMs: 1000 });
    expect(h2.mode).toBe("read");
    expect(writer.done).toBe(false); // writer still starved while readers present

    await r0.release(h0);
    await r2.release(h2);
    await settle(300);
    expect(writer.done && writer.value?.mode === "write").toBe(true);
    await wr.release(writer.value!);
  });
});

describe("M2 fairness — fifo & write_preferring (no writer starvation)", () => {
  for (const fairness of ["fifo", "write_preferring"] as const) {
    it(`${fairness}: a new reader cannot jump a queued writer; the writer is served`, async () => {
      const r1 = await mk();
      const wr = await mk();
      const r2 = await mk();

      const h1 = await r1.acquireRead("f", { ownerId: "R1", fairness, leaseMs: 30_000 });
      const writer = track(wr.acquireWrite("f", { ownerId: "W1", fairness, leaseMs: 30_000, waitMs: 5000 }));
      await settle(150);

      // a new reader must wait behind the queued writer -> times out
      await expect(
        r2.acquireRead("f", { ownerId: "R2", fairness, leaseMs: 30_000, waitMs: 300 }),
      ).rejects.toBeInstanceOf(WaitTimeoutError);

      // the holding reader releases -> writer is granted (not starved)
      await r1.release(h1);
      await settle(250);
      expect(writer.done && writer.value?.mode === "write").toBe(true);
      await wr.release(writer.value!);
    });
  }
});

describe("M2 ghost-grant race (SPEC §20.5)", () => {
  it("never leaks when a wait timeout coincides with a grant", async () => {
    const w = await mk();
    const other = await mk();
    const res = "ghost";

    for (let i = 0; i < 25; i++) {
      const hw = await w.acquireWrite(res, { ownerId: "w", leaseMs: 30_000 });
      const waitMs = 30 + (i % 15); // vary the buzzer so it lands near the release
      const waiter = other
        .acquireWrite(res, { ownerId: "o", leaseMs: 30_000, waitMs })
        .then((h) => ({ h }))
        .catch((e: unknown) => ({ e }));

      await settle(waitMs); // release right around the waiter's deadline
      await w.release(hw);

      const result = await waiter;
      if ("h" in result && result.h) {
        await other.release(result.h); // taken just in time -> release it
      } else {
        expect(result).toHaveProperty("e");
        expect((result as { e: unknown }).e).toBeInstanceOf(WaitTimeoutError);
      }

      // invariant: either taken-and-released or fully reclaimed — never leaked.
      const s = await stateOf(res);
      expect(s.holders, `iteration ${i}: holder leaked`).toBe(0);
      expect(s.mode, `iteration ${i}: stale mode`).toBe("none");
    }
  });
});
