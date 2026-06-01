import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    testTimeout: 20000,
    hookTimeout: 30000,
    // Each test file spawns its own redis-server on a random port; keep files
    // from racing on shared ports / process teardown.
    fileParallelism: false,
  },
});
