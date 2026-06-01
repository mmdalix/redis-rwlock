// Spawns a throwaway redis-server on a random port for tests. Uses the locally
// installed redis-server (no Docker needed). Each test file gets its own instance.

import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import Redis from "ioredis";

export interface RedisHarness {
  port: number;
  client: () => Redis;
  flush: () => Promise<void>;
  stop: () => Promise<void>;
}

function randomPort(): number {
  return 20000 + Math.floor(Math.random() * 20000);
}

async function waitForReady(port: number, timeoutMs = 10000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const probe = new Redis({ port, lazyConnect: true, maxRetriesPerRequest: 1 });
    try {
      await probe.connect();
      const pong = await probe.ping();
      probe.disconnect();
      if (pong === "PONG") return;
    } catch {
      probe.disconnect();
    }
    if (Date.now() > deadline) throw new Error(`redis-server on :${port} did not become ready`);
    await new Promise((r) => setTimeout(r, 100));
  }
}

export async function startRedis(): Promise<RedisHarness> {
  const port = randomPort();
  const dir = mkdtempSync(join(tmpdir(), "rwlock-redis-"));
  const proc: ChildProcess = spawn(
    "redis-server",
    ["--port", String(port), "--save", "", "--appendonly", "no", "--dir", dir],
    { stdio: "ignore" },
  );
  proc.on("error", (e) => {
    throw new Error(`failed to spawn redis-server: ${e.message}`);
  });

  await waitForReady(port);

  const clients: Redis[] = [];
  const client = () => {
    const c = new Redis({ port, maxRetriesPerRequest: 2 });
    clients.push(c);
    return c;
  };

  return {
    port,
    client,
    flush: async () => {
      const c = client();
      await c.flushall();
      c.disconnect();
    },
    stop: async () => {
      for (const c of clients) c.disconnect();
      proc.kill("SIGKILL");
      await new Promise((r) => setTimeout(r, 100));
      rmSync(dir, { recursive: true, force: true });
    },
  };
}
