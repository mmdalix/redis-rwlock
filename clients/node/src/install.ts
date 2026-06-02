// Module install + cross-version handshake (SPEC §16, §17). On connect we read the
// global `rwlock:__module__` marker; if the server speaks an incompatible protocol
// MAJOR we refuse to contend (IncompatibleServerLogicError) rather than silently use
// foreign semantics. Otherwise we pick a delivery (Functions if available, else
// EVALSHA) and write the marker if it is absent. We NEVER require an admin command:
// FUNCTION LOAD is attempted, and any failure degrades silently to scripts.

import { createHash } from "node:crypto";
import { FUNCTIONS, PROTOCOL_VERSION } from "./lua.generated.js";
import { type Delivery, type DeliveryClient, FunctionDelivery, ScriptDelivery } from "./delivery.js";
import { IncompatibleServerLogicError, UnsupportedError } from "./errors.js";

export const MODULE_KEY = "rwlock:__module__";
export const IMPL_VERSION = "0.0.0";
/** SHA of the canonical FUNCTION library — same protocol bytes ⇒ same behavior. */
export const MODULE_SHA = createHash("sha1").update(FUNCTIONS).digest("hex");

export interface InstallClient extends DeliveryClient {
  hGetAll(key: string): Promise<unknown>;
  hSet(key: string, values: Record<string, string>): Promise<unknown>;
}

export type DeliveryPreference = "auto" | "functions" | "scripts";

export interface ModuleInfo {
  protocolVersion: number;
  implVersion: string;
  sha: string;
}

async function readMarker(client: InstallClient): Promise<Record<string, string> | undefined> {
  const raw = (await client.hGetAll(MODULE_KEY)) as Record<string, string>;
  return raw && Object.keys(raw).length > 0 ? raw : undefined;
}

async function tryFunctionLoad(client: InstallClient): Promise<boolean> {
  try {
    await client.sendCommand(["FUNCTION", "LOAD", "REPLACE", FUNCTIONS]);
    return true;
  } catch {
    return false; // FUNCTION unavailable/denied (e.g. managed Redis) -> use scripts
  }
}

export interface InstallResult {
  delivery: Delivery;
  info: ModuleInfo;
}

export async function installAndHandshake(
  client: InstallClient,
  opts: { delivery: DeliveryPreference; allowIncompatibleProtocol: boolean },
): Promise<InstallResult> {
  const marker = await readMarker(client);
  if (marker) {
    const serverProto = Number(marker.protocol_version);
    if (serverProto !== PROTOCOL_VERSION && !opts.allowIncompatibleProtocol) {
      throw new IncompatibleServerLogicError(
        `Redis has redis-rwlock protocol v${serverProto}; this client speaks v${PROTOCOL_VERSION}. ` +
          `Different protocol majors must not share a resource namespace (set allowIncompatibleProtocol to override).`,
      );
    }
  }

  let delivery: Delivery | undefined;
  if (opts.delivery !== "scripts") {
    if (await tryFunctionLoad(client)) {
      delivery = new FunctionDelivery(client);
    } else if (opts.delivery === "functions") {
      throw new UnsupportedError("Redis Functions (FUNCTION LOAD) is not available on this server");
    }
  }
  if (!delivery) delivery = new ScriptDelivery(client);

  if (!marker) {
    // Idempotent and safe under concurrent installers (all write identical values).
    await client.hSet(MODULE_KEY, {
      protocol_version: String(PROTOCOL_VERSION),
      impl_version: IMPL_VERSION,
      sha: MODULE_SHA,
      loaded_at_ms: String(Date.now()),
    });
  }

  return {
    delivery,
    info: {
      protocolVersion: marker ? Number(marker.protocol_version) : PROTOCOL_VERSION,
      implVersion: marker?.impl_version ?? IMPL_VERSION,
      sha: marker?.sha ?? MODULE_SHA,
    },
  };
}
