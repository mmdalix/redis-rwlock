import { defineConfig } from "tsdown";

// tsdown (Rolldown-powered) is the modern successor to tsup for library bundling.
export default defineConfig({
  entry: ["src/index.ts"],
  format: ["esm", "cjs"],
  dts: true,
  clean: true,
  sourcemap: true,
  target: "node22",
});
