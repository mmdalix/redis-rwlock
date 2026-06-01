// Lua delivery for node-redis. Unlike ioredis, node-redis only attaches scripts
// at createClient() time, but we wrap the user's *existing* client — so we manage
// EVALSHA + SCRIPT LOAD + NOSCRIPT fallback ourselves. This also gives us the
// delivery control SPEC §17 calls for (and the seam for the Functions path in M5).

import { SCRIPTS, type ScriptName } from "./lua.generated.js";

// Structural subset of a node-redis client that we depend on. Any v5/v6 client
// (RedisClientType) satisfies this; we avoid the heavy generics deliberately.
export interface RedisLike {
  evalSha(sha: string, options: { keys: string[]; arguments: string[] }): Promise<unknown>;
  scriptLoad(script: string): Promise<unknown>;
}

function isNoScript(err: unknown): boolean {
  return err instanceof Error && err.message.toUpperCase().includes("NOSCRIPT");
}

/**
 * Runs the shared Lua via EVALSHA, loading on first use and transparently
 * recovering from NOSCRIPT (e.g. after a server SCRIPT FLUSH or restart).
 */
export class ScriptRunner {
  private readonly client: RedisLike;
  private readonly shas = new Map<ScriptName, string>();

  constructor(client: RedisLike) {
    this.client = client;
  }

  private async load(name: ScriptName): Promise<string> {
    const sha = String(await this.client.scriptLoad(SCRIPTS[name]));
    this.shas.set(name, sha);
    return sha;
  }

  async run(name: ScriptName, keys: string[], args: string[]): Promise<unknown> {
    const sha = this.shas.get(name) ?? (await this.load(name));
    try {
      return await this.client.evalSha(sha, { keys, arguments: args });
    } catch (err) {
      if (!isNoScript(err)) throw err;
      return await this.client.evalSha(await this.load(name), { keys, arguments: args });
    }
  }
}
