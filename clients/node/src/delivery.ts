// Two interchangeable ways to run the shared Lua, chosen by capability probe:
//   * FunctionDelivery — Redis FUNCTION library + FCALL (when FUNCTION LOAD works).
//   * ScriptDelivery   — SCRIPT LOAD + EVALSHA with NOSCRIPT fallback (always works).
// Same source either way, so semantics are identical (SPEC §3.5, §17).

import { SCRIPTS, type ScriptName } from "./lua.generated.js";

export interface DeliveryClient {
  evalSha(sha: string, options: { keys: string[]; arguments: string[] }): Promise<unknown>;
  scriptLoad(script: string): Promise<unknown>;
  fCall(name: string, options: { keys: string[]; arguments: string[] }): Promise<unknown>;
  sendCommand(args: string[]): Promise<unknown>;
}

export interface Delivery {
  readonly mode: "functions" | "scripts";
  run(name: ScriptName, keys: string[], args: string[]): Promise<unknown>;
}

const FN_NAME: Record<ScriptName, string> = {
  acquire: "rwlock_acquire",
  release: "rwlock_release",
  extend: "rwlock_extend",
  cancelWait: "rwlock_cancel_wait",
  expireAndGrant: "rwlock_expire_and_grant",
  inspect: "rwlock_inspect",
};

function isNoScript(err: unknown): boolean {
  return err instanceof Error && err.message.toUpperCase().includes("NOSCRIPT");
}

/** EVALSHA delivery: loads on first use, recovers transparently from NOSCRIPT. */
export class ScriptDelivery implements Delivery {
  readonly mode = "scripts" as const;
  private readonly client: DeliveryClient;
  private readonly shas = new Map<ScriptName, string>();

  constructor(client: DeliveryClient) {
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
      return this.client.evalSha(await this.load(name), { keys, arguments: args });
    }
  }
}

/** FCALL delivery against the installed `rwlock` FUNCTION library. */
export class FunctionDelivery implements Delivery {
  readonly mode = "functions" as const;
  private readonly client: DeliveryClient;

  constructor(client: DeliveryClient) {
    this.client = client;
  }

  run(name: ScriptName, keys: string[], args: string[]): Promise<unknown> {
    // fCall (not a raw sendCommand) so node-redis routes by key on a Cluster client.
    return this.client.fCall(FN_NAME[name], { keys, arguments: args });
  }
}
