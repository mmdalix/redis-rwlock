import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { RwLock, WaitTimeout } from "../src/index.js";
import { startRedis, type RedisHarness } from "./redis-harness.js";

// M1: read locks — concurrent readers, contiguous-reader batching on a single
// grant, stop-at-queued-writer, the max_reader_batch cap, and state-cache accuracy.
// (Full fairness-policy distinctions — fifo vs read_preferring — are hardened in M2.)

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

/** Track a promise's settled state without awaiting it. */
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

/** Read the denormalized state cache for assertions. */
async function readState(r: string): Promise<{ mode: string; readerCount: number; queuedWriters: number }> {
  const client = await harness.newClient();
  const h = (await client.hGetAll(`rwlock:{${r}}:state`)) as Record<string, string>;
  return {
    mode: h.mode ?? "none",
    readerCount: Number(h.reader_count ?? 0),
    queuedWriters: Number(h.queued_writers ?? 0),
  };
}

describe("M1 read locks", () => {
  it("lets multiple readers hold concurrently with increasing fencing", async () => {
    const rw = await mk();
    const a = await rw.acquireRead("r", { ownerId: "r1", leaseMs: 30_000 });
    const b = await rw.acquireRead("r", { ownerId: "r2", leaseMs: 30_000 });
    const c = await rw.acquireRead("r", { ownerId: "r3", leaseMs: 30_000 });
    expect([a, b, c].every((h) => h.mode === "read")).toBe(true);
    expect(b.fencingToken).toBeGreaterThan(a.fencingToken);
    expect(c.fencingToken).toBeGreaterThan(b.fencingToken);
    await Promise.all([rw.release(a), rw.release(b), rw.release(c)]);
  });

  it("keeps the state cache accurate across reader add/remove and mode transitions", async () => {
    const rw = await mk();
    expect((await readState("r")).mode).toBe("none");

    const a = await rw.acquireRead("r", { ownerId: "r1", leaseMs: 30_000 });
    const b = await rw.acquireRead("r", { ownerId: "r2", leaseMs: 30_000 });
    let s = await readState("r");
    expect(s.mode).toBe("read");
    expect(s.readerCount).toBe(2);

    await rw.release(a);
    s = await readState("r");
    expect(s.readerCount).toBe(1);

    await rw.release(b);
    s = await readState("r");
    expect(s.mode).toBe("none");
    expect(s.readerCount).toBe(0);
  });

  it("blocks a writer while readers hold, then grants it once all readers release", async () => {
    const r1 = await mk();
    const r2 = await mk();
    const w = await mk();
    const h1 = await r1.acquireRead("res", { ownerId: "r1", leaseMs: 30_000 });
    const h2 = await r2.acquireRead("res", { ownerId: "r2", leaseMs: 30_000 });

    const writer = track(w.acquireWrite("res", { ownerId: "w1", leaseMs: 30_000, waitMs: 5000 }));
    await settle(150);
    expect(writer.done).toBe(false); // readers still hold

    await r1.release(h1);
    await settle(150);
    expect(writer.done).toBe(false); // one reader still holds

    await r2.release(h2);
    await settle(250);
    expect(writer.done).toBe(true); // last reader gone -> writer granted
    expect(writer.value!.mode).toBe("write");
    await w.release(writer.value!);
  });

  it("batch-grants all contiguous queued readers in a single writer release", async () => {
    const w = await mk();
    const readers = await Promise.all([mk(), mk(), mk()]);
    const hw = await w.acquireWrite("res", { ownerId: "w0", leaseMs: 30_000 });

    // three readers queue behind the writer
    const waits = readers.map((rw, i) =>
      track(rw.acquireRead("res", { ownerId: `r${i}`, leaseMs: 30_000, waitMs: 5000 })),
    );
    await settle(150);
    expect(waits.every((x) => !x.done)).toBe(true);

    const start = Date.now();
    await w.release(hw);
    await settle(300);

    // all three woken by the single release, co-holding
    expect(waits.every((x) => x.done && x.value?.mode === "read")).toBe(true);
    expect(Date.now() - start).toBeLessThan(1500);
    const s = await readState("res");
    expect(s.readerCount).toBe(3);

    await Promise.all(readers.map((rw, i) => rw.release(waits[i]!.value!)));
  });

  it("stops a reader batch at a queued writer (SPEC §6.2 worked example)", async () => {
    // Queue order R1 R2 W2 R3 behind a holding writer W1.
    const [w1, r1, r2, w2, r3] = await Promise.all([mk(), mk(), mk(), mk(), mk()]);
    const hw1 = await w1.acquireWrite("x", { ownerId: "W1", leaseMs: 30_000 });

    const a1 = track(r1!.acquireRead("x", { ownerId: "R1", leaseMs: 30_000, waitMs: 8000 }));
    await settle(40);
    const a2 = track(r2!.acquireRead("x", { ownerId: "R2", leaseMs: 30_000, waitMs: 8000 }));
    await settle(40);
    const b2 = track(w2!.acquireWrite("x", { ownerId: "W2", leaseMs: 30_000, waitMs: 8000 }));
    await settle(40);
    const a3 = track(r3!.acquireRead("x", { ownerId: "R3", leaseMs: 30_000, waitMs: 8000 }));
    await settle(120);

    // W1 releases -> R1, R2 batch-granted; W2 and R3 wait (R3 must not jump W2).
    await w1.release(hw1);
    await settle(300);
    expect(a1.done && a2.done).toBe(true);
    expect(b2.done).toBe(false);
    expect(a3.done).toBe(false);
    expect((await readState("x")).readerCount).toBe(2);

    // R1 and R2 release -> W2 granted; R3 still waits.
    await r1!.release(a1.value!);
    await r2!.release(a2.value!);
    await settle(300);
    expect(b2.done).toBe(true);
    expect(b2.value!.mode).toBe("write");
    expect(a3.done).toBe(false);

    // W2 releases -> R3 granted.
    await w2!.release(b2.value!);
    await settle(300);
    expect(a3.done).toBe(true);
    expect(a3.value!.mode).toBe("read");
    await r3!.release(a3.value!);
  });

  it("caps a single grant at max_reader_batch; remaining readers drain on the next transition", async () => {
    const w = await mk();
    const readers = await Promise.all([mk(), mk(), mk(), mk()]);
    const hw = await w.acquireWrite("cap", { ownerId: "w0", leaseMs: 30_000 });

    const waits = readers.map((rw, i) =>
      track(rw.acquireRead("cap", { ownerId: `r${i}`, leaseMs: 30_000, waitMs: 8000, maxReaderBatch: 2 })),
    );
    await settle(150);

    await w.release(hw);
    await settle(300);

    // exactly the cap (2) are granted in the release transition
    const grantedFirst = waits.filter((x) => x.done).length;
    expect(grantedFirst).toBe(2);
    expect((await readState("cap")).readerCount).toBe(2);

    // releasing a held reader triggers grant_from_queue -> remaining readers drain
    const firstHeld = waits.find((x) => x.done)!;
    const firstHeldIdx = waits.indexOf(firstHeld);
    await readers[firstHeldIdx]!.release(firstHeld.value!);
    await settle(300);

    expect(waits.every((x) => x.done && !x.err)).toBe(true);
    // release everyone still holding
    await Promise.all(
      waits
        .filter((x) => x !== firstHeld && x.value)
        .map((x, i) => readers[waits.indexOf(x)]!.release(x.value!)),
    );
  });

  it("write_preferring: a queued writer blocks new readers from jumping ahead", async () => {
    const r1 = await mk();
    const wr = await mk();
    const r2 = await mk();
    const h1 = await r1.acquireRead("p", { ownerId: "r1", leaseMs: 30_000 });

    const writerWait = wr.acquireWrite("p", { ownerId: "w1", leaseMs: 30_000, waitMs: 5000 });
    await settle(150);
    expect((await readState("p")).queuedWriters).toBe(1);

    // a new reader must NOT jump ahead of the queued writer -> times out
    await expect(
      r2.acquireRead("p", { ownerId: "r2", leaseMs: 30_000, waitMs: 300 }),
    ).rejects.toBeInstanceOf(WaitTimeout);

    await r1.release(h1);
    const hw = await writerWait;
    expect(hw.mode).toBe("write");
    await wr.release(hw);
  });
});
