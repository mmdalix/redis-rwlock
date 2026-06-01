import { readFileSync, readdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { RwLock, WaitTimeoutError, type LockHandle } from "../src/index.js";
import { startRedis, type RedisHarness } from "./redis-harness.js";

// Runs the shared, language-agnostic conformance scenarios (protocol/conformance)
// against this client. Every language port must pass the same files (SPEC §20.10).

const here = dirname(fileURLToPath(import.meta.url));
const scenarioDir = resolve(here, "../../../protocol/conformance/scenarios");

interface Step {
  op: "acquire" | "release" | "expectFencingGt";
  as?: string;
  handle?: string;
  than?: string;
  resource?: string;
  mode?: "read" | "write";
  leaseMs?: number;
  waitMs?: number;
  expect?: "granted" | "timeout";
}
interface Scenario {
  name: string;
  steps: Step[];
}

function loadScenarios(): Scenario[] {
  return readdirSync(scenarioDir)
    .filter((f) => f.endsWith(".json"))
    .map((f) => JSON.parse(readFileSync(join(scenarioDir, f), "utf8")) as Scenario);
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

describe("cross-language conformance scenarios", () => {
  for (const scenario of loadScenarios()) {
    it(scenario.name, async () => {
      const client = await harness.newClient();
      const rw = new RwLock(client, { requireOwnerId: false });
      locks.push(rw);
      const handles = new Map<string, LockHandle>();
      let ns = 0;
      const nsResource = (r: string) => `conf:${scenario.name}:${r}`;

      for (const step of scenario.steps) {
        if (step.op === "acquire") {
          const resource = nsResource(step.resource!);
          const p = rw.acquire(resource, step.mode!, {
            ownerId: `c${ns++}`,
            leaseMs: step.leaseMs,
            waitMs: step.waitMs,
          });
          if (step.expect === "timeout") {
            await expect(p).rejects.toBeInstanceOf(WaitTimeoutError);
          } else {
            const h = await p;
            if (step.as) handles.set(step.as, h);
          }
        } else if (step.op === "release") {
          const h = handles.get(step.handle!);
          expect(h, `handle ${step.handle} not found`).toBeDefined();
          await rw.release(h!);
        } else if (step.op === "expectFencingGt") {
          const a = handles.get(step.handle!);
          const b = handles.get(step.than!);
          expect(a && b, `handles for fencing comparison not found`).toBeTruthy();
          expect(a!.fencingToken).toBeGreaterThan(b!.fencingToken);
        }
      }
    });
  }
});
