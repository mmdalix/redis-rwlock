// gen-lua.mjs — embed the canonical Lua (protocol/lua) into a TypeScript module.
//
// The shared Lua is the single source of truth. A published npm package cannot
// reference files outside its own directory, so we vendor the scripts as string
// constants in src/lua.generated.ts. lib.lua is prepended to each script so the
// helpers are in scope (the scripts are loaded as standalone EVALSHA bodies).
//
// Run with: npm run gen:lua   (also runs automatically before build)

import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const luaDir = resolve(here, "../../../protocol/lua");
const outFile = resolve(here, "../src/lua.generated.ts");

const SCRIPTS = ["acquire", "release", "extend", "cancel_wait", "expire_and_grant"];

const camel = (s) => s.replace(/_([a-z])/g, (_, c) => c.toUpperCase());

const lib = readFileSync(join(luaDir, "lib.lua"), "utf8");

const entries = SCRIPTS.map((name) => {
  const body = readFileSync(join(luaDir, `${name}.lua`), "utf8");
  const combined = `${lib}\n${body}`;
  return `  ${camel(name)}: ${JSON.stringify(combined)},`;
}).join("\n");

const out =
  "// GENERATED FROM protocol/lua — DO NOT EDIT. Run `npm run gen:lua` to regenerate.\n" +
  "/* eslint-disable */\n" +
  "export const SCRIPTS = {\n" +
  entries +
  "\n} as const;\n\n" +
  "export type ScriptName = keyof typeof SCRIPTS;\n";

mkdirSync(dirname(outFile), { recursive: true });
writeFileSync(outFile, out);
console.log(`Wrote ${outFile} (${SCRIPTS.length} scripts)`);
