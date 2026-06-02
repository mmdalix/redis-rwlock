// gen-lua.mjs — embed the canonical Lua (protocol/lua) into a TypeScript module.
//
// The shared Lua is the single source of truth. A published npm package cannot
// reference files outside its own directory, so we vendor it as string constants
// in src/lua.generated.ts. Two delivery shapes are produced from the SAME bodies:
//   * SCRIPTS:   lib.lua + each body, loaded via SCRIPT LOAD / EVALSHA.
//   * FUNCTIONS: a single Redis FUNCTION library (#!lua name=rwlock) that wraps each
//                body in redis.register_function (KEYS/ARGV bound from keys/args).
// PROTOCOL_VERSION is read from protocol/VERSION.
//
// Run with: npm run gen:lua   (also runs automatically before build)

import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const luaDir = resolve(here, "../../../protocol/lua");
const versionFile = resolve(here, "../../../protocol/VERSION");
const outFile = resolve(here, "../src/lua.generated.ts");

const SCRIPTS = ["acquire", "release", "extend", "cancel_wait", "expire_and_grant", "inspect"];

const camel = (s) => s.replace(/_([a-z])/g, (_, c) => c.toUpperCase());

const protocolVersion = parseInt(readFileSync(versionFile, "utf8").trim(), 10);
const lib = readFileSync(luaDir + "/lib.lua", "utf8");
const bodies = Object.fromEntries(SCRIPTS.map((n) => [n, readFileSync(join(luaDir, `${n}.lua`), "utf8")]));

// EVALSHA bodies: lib + body, each a standalone script.
const scriptEntries = SCRIPTS.map(
  (name) => `  ${camel(name)}: ${JSON.stringify(`${lib}\n${bodies[name]}`)},`,
).join("\n");

// Functions library: lib once, then a registered function per body. Each callback
// binds KEYS/ARGV from (keys, args) so the unmodified body works verbatim.
const wrappers = SCRIPTS.map((name) => {
  const callback = `function(keys, args)\n  local KEYS, ARGV = keys, args\n${bodies[name]}\nend`;
  // inspect is read-only -> register with the no-writes flag so it can run on replicas.
  if (name === "inspect") {
    return `redis.register_function{ function_name = 'rwlock_inspect', flags = { 'no-writes' }, callback = ${callback} }`;
  }
  return `redis.register_function('rwlock_${name}', ${callback})`;
}).join("\n\n");
const functionLib = `#!lua name=rwlock\n${lib}\n${wrappers}\n`;

const out =
  "// GENERATED FROM protocol/lua — DO NOT EDIT. Run `npm run gen:lua` to regenerate.\n" +
  "/* eslint-disable */\n" +
  `export const PROTOCOL_VERSION = ${protocolVersion};\n\n` +
  "export const SCRIPTS = {\n" +
  scriptEntries +
  "\n} as const;\n\n" +
  "export type ScriptName = keyof typeof SCRIPTS;\n\n" +
  `export const FUNCTIONS = ${JSON.stringify(functionLib)};\n`;

mkdirSync(dirname(outFile), { recursive: true });
writeFileSync(outFile, out);
console.log(`Wrote ${outFile} (PROTOCOL_VERSION=${protocolVersion}, ${SCRIPTS.length} scripts + FUNCTION library)`);
