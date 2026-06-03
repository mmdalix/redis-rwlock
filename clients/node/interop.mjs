// Node side of the cross-language interop test (not published — see package.json files).
// Usage: node interop.mjs <addr> <write|read> <resource> <waitMs>
// Output protocol matches the Go interop CLI: "RESULT FENCING <n>" | "RESULT TIMEOUT".
import { createClient } from "redis";
import { RwLock, WaitTimeoutError } from "./dist/index.mjs";

const [addr, mode, resource, waitMs] = process.argv.slice(2);
const client = await createClient({ url: `redis://${addr}` }).connect();
const rw = new RwLock(client);
try {
  const acquire = mode === "read" ? rw.acquireRead.bind(rw) : rw.acquireWrite.bind(rw);
  const h = await acquire(resource, { waitMs: Number(waitMs), ownerId: "node-interop" });
  console.log("RESULT FENCING", h.fencingToken);
  await rw.release(h);
} catch (e) {
  if (e instanceof WaitTimeoutError) console.log("RESULT TIMEOUT");
  else {
    console.log("RESULT ERROR", e?.message ?? e);
    process.exitCode = 1;
  }
} finally {
  await rw.close();
  await client.close();
}
