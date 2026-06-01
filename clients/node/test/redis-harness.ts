// Spawns a throwaway redis-server on a random port for tests. Uses the locally
// installed redis-server (no Docker needed). Each test file gets its own instance.

import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createClient, type RedisClientType } from "redis";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Client = RedisClientType<any, any, any, any, any>;

export interface RedisHarness {
  url: string;
  /** A fresh, connected node-redis client (tracked for teardown). */
  newClient: () => Promise<Client>;
  flush: () => Promise<void>;
  stop: () => Promise<void>;
}

function randomPort(): number {
  return 20000 + Math.floor(Math.random() * 20000);
}

async function waitForReady(url: string, timeoutMs = 10000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const probe = createClient({ url });
    probe.on("error", () => {});
    try {
      await probe.connect();
      const pong = await probe.ping();
      await probe.close();
      if (pong === "PONG") return;
    } catch {
      probe.destroy?.();
    }
    if (Date.now() > deadline) throw new Error(`redis-server at ${url} did not become ready`);
    await new Promise((r) => setTimeout(r, 100));
  }
}

export async function startRedis(): Promise<RedisHarness> {
  const port = randomPort();
  const url = `redis://127.0.0.1:${port}`;
  const dir = mkdtempSync(join(tmpdir(), "rwlock-redis-"));
  const proc: ChildProcess = spawn(
    "redis-server",
    ["--port", String(port), "--save", "", "--appendonly", "no", "--dir", dir],
    { stdio: "ignore" },
  );
  proc.on("error", (e) => {
    throw new Error(`failed to spawn redis-server: ${e.message}`);
  });

  await waitForReady(url);

  const clients: Client[] = [];
  const newClient = async (): Promise<Client> => {
    const c = createClient({ url }) as Client;
    c.on("error", () => {});
    await c.connect();
    clients.push(c);
    return c;
  };

  return {
    url,
    newClient,
    flush: async () => {
      const c = await newClient();
      await c.flushAll();
    },
    stop: async () => {
      await Promise.all(clients.map((c) => c.close().catch(() => c.destroy?.())));
      proc.kill("SIGKILL");
      await new Promise((r) => setTimeout(r, 100));
      rmSync(dir, { recursive: true, force: true });
    },
  };
}
