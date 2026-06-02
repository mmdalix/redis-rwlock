import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
  RwLock,
  WaitTimeoutError,
  type Metrics,
  type RwLockConfig,
  type Span,
  type Tracer,
} from "../src/index.js";
import { startRedis, type RedisHarness } from "./redis-harness.js";

// M6: observability (metrics, tracing, inspect) + the blocking-connection gauge.

const settle = (ms: number) => new Promise((r) => setTimeout(r, ms));

type Sample = { name: string; value?: number; labels?: Record<string, string | number> };

class CapturingMetrics implements Metrics {
  counters: Sample[] = [];
  observations: Sample[] = [];
  gauges: Sample[] = [];
  increment(name: string, labels?: Record<string, string | number>) {
    this.counters.push({ name, labels });
  }
  observe(name: string, value: number, labels?: Record<string, string | number>) {
    this.observations.push({ name, value, labels });
  }
  gauge(name: string, value: number, labels?: Record<string, string | number>) {
    this.gauges.push({ name, value, labels });
  }
  count(name: string, label?: [string, string | number]): number {
    return this.counters.filter(
      (c) => c.name === name && (!label || c.labels?.[label[0]] === label[1]),
    ).length;
  }
}

class CapSpan implements Span {
  attributes: Record<string, string | number | boolean> = {};
  status?: boolean;
  exceptions: unknown[] = [];
  ended = false;
  setAttribute(k: string, v: string | number | boolean) {
    this.attributes[k] = v;
  }
  recordException(e: unknown) {
    this.exceptions.push(e);
  }
  setStatus(ok: boolean) {
    this.status = ok;
  }
  end() {
    this.ended = true;
  }
}
class CapturingTracer implements Tracer {
  spans: { name: string; span: CapSpan }[] = [];
  startSpan(name: string): Span {
    const span = new CapSpan();
    this.spans.push({ name, span });
    return span;
  }
  byName(name: string): CapSpan[] {
    return this.spans.filter((s) => s.name === name).map((s) => s.span);
  }
}

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

describe("M6 inspect()", () => {
  it("reports a writer hold and then a free resource", async () => {
    const rw = await mk();
    const h = await rw.acquireWrite("i", { ownerId: "w1", leaseMs: 30_000 });
    let s = await rw.inspect("i");
    expect(s.mode).toBe("write");
    expect(s.writerActive).toBe(true);
    expect(s.readerCount).toBe(0);
    expect(s.nextExpiryMs).toBeGreaterThan(0);
    expect(s.nextExpiryMs).toBeLessThanOrEqual(30_000);

    await rw.release(h);
    s = await rw.inspect("i");
    expect(s.mode).toBe("none");
    expect(s.nextExpiryMs).toBe(-1);
  });

  it("reports reader count and a queued writer with its wait age", async () => {
    const r1 = await mk();
    const r2 = await mk();
    const w = await mk();
    await r1.acquireRead("i2", { ownerId: "r1", leaseMs: 30_000 });
    await r2.acquireRead("i2", { ownerId: "r2", leaseMs: 30_000 });
    const waiter = w.acquireWrite("i2", { ownerId: "w1", leaseMs: 30_000, waitMs: 5000 });
    await settle(200);

    const s = await r1.inspect("i2");
    expect(s.mode).toBe("read");
    expect(s.readerCount).toBe(2);
    expect(s.queueLength).toBe(1);
    expect(s.queuedWriters).toBe(1);
    expect(s.oldestWaitMs).toBeGreaterThanOrEqual(0);

    await expect(Promise.race([waiter, settle(50).then(() => "pending")])).resolves.toBe("pending");
  });
});

describe("M6 metrics", () => {
  it("emits granted acquire, wait/held durations, fencing gauge, and release", async () => {
    const metrics = new CapturingMetrics();
    const rw = await mk({ metrics });
    const h = await rw.acquireWrite("m", { ownerId: "w1", leaseMs: 30_000 });
    await rw.release(h);

    expect(metrics.count("rwlock_acquire_total", ["result", "granted"])).toBe(1);
    expect(metrics.observations.some((o) => o.name === "rwlock_wait_duration_ms")).toBe(true);
    expect(metrics.observations.some((o) => o.name === "rwlock_held_duration_ms")).toBe(true);
    expect(metrics.gauges.some((g) => g.name === "rwlock_fencing_token_current")).toBe(true);
    expect(metrics.count("rwlock_release_total", ["result", "ok"])).toBe(1);
  });

  it("counts timeouts", async () => {
    const metrics = new CapturingMetrics();
    const a = await mk();
    const b = await mk({ metrics });
    await a.acquireWrite("m2", { ownerId: "w1", leaseMs: 30_000 });
    await expect(b.acquireWrite("m2", { ownerId: "w2", waitMs: 250 })).rejects.toBeInstanceOf(
      WaitTimeoutError,
    );
    expect(metrics.count("rwlock_acquire_total", ["result", "timeout"])).toBe(1);
    expect(metrics.count("rwlock_timeouts_total")).toBe(1);
  });

  it("counts extend results and lock loss", async () => {
    const metrics = new CapturingMetrics();
    const rw = await mk({ metrics });
    const h = await rw.acquireWrite("m3", { ownerId: "w1", leaseMs: 30_000 });
    await rw.extend(h, 60_000);
    expect(metrics.count("rwlock_extend_total", ["result", "ok"])).toBe(1);

    // force-evict the holder while the lease is still far (so the margin guard passes
    // and the server itself reports LOST).
    const h2 = await rw.acquireWrite("m3b", { ownerId: "w1", leaseMs: 30_000 });
    const admin = await harness.newClient();
    await admin.del(`rwlock:{m3b}:holders`);
    await admin.del(`rwlock:{m3b}:holder_meta`);
    await expect(rw.extend(h2, 60_000)).rejects.toThrow();
    expect(metrics.count("rwlock_extend_total", ["result", "lost"])).toBe(1);
    expect(metrics.count("rwlock_lock_lost_total")).toBe(1);
  });

  it("tracks the blocking-connection gauge up and back to zero", async () => {
    const metrics = new CapturingMetrics();
    const a = await mk();
    const b = await mk({ metrics });
    await a.acquireWrite("bp", { ownerId: "w1", leaseMs: 30_000 });
    const waiter = b.acquireWrite("bp", { ownerId: "w2", waitMs: 700 }).catch(() => {});
    await settle(200);
    expect(b.blockingConnectionsInUse).toBe(1);
    expect(metrics.gauges.some((g) => g.name === "rwlock_blocking_connections_in_use" && g.value === 1)).toBe(
      true,
    );
    await waiter;
    expect(b.blockingConnectionsInUse).toBe(0);
  });
});

describe("M6 tracing", () => {
  it("creates acquire and hold spans with attributes and outcomes", async () => {
    const tracer = new CapturingTracer();
    const rw = await mk({ tracer });
    const h = await rw.acquireWrite("t", { ownerId: "w1", leaseMs: 30_000 });
    await rw.release(h);

    const acquire = tracer.byName("rwlock.acquire");
    expect(acquire).toHaveLength(1);
    expect(acquire[0]!.status).toBe(true);
    expect(acquire[0]!.ended).toBe(true);
    expect(acquire[0]!.attributes.fencingToken).toBe(h.fencingToken);

    const hold = tracer.byName("rwlock.hold");
    expect(hold).toHaveLength(1);
    expect(hold[0]!.ended).toBe(true); // ended on release
  });

  it("records an exception on the acquire span when it times out", async () => {
    const tracer = new CapturingTracer();
    const a = await mk();
    const b = await mk({ tracer });
    await a.acquireWrite("t2", { ownerId: "w1", leaseMs: 30_000 });
    await expect(b.acquireWrite("t2", { ownerId: "w2", waitMs: 250 })).rejects.toBeInstanceOf(
      WaitTimeoutError,
    );
    const acquire = tracer.byName("rwlock.acquire");
    expect(acquire[0]!.status).toBe(false);
    expect(acquire[0]!.exceptions.length).toBe(1);
    expect(acquire[0]!.ended).toBe(true);
  });
});
