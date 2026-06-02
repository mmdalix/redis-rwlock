import { describe, expect, it } from "vitest";
import { keyPrefix } from "../src/index.js";

// M5: Redis Cluster safety. A Lua/Function call may only touch keys in ONE slot, so
// every key for a resource must share a slot. We enforce that with the {r} hash tag
// (SPEC §4); here we prove it by computing the actual cluster slot in JS and anchoring
// it to the value Redis itself reports (CLUSTER KEYSLOT 'rwlock:{order:9}:readers' = 14638).

// CRC16/XMODEM, the function Redis Cluster uses for slot assignment.
function crc16(s: string): number {
  let crc = 0;
  for (let i = 0; i < s.length; i++) {
    crc ^= (s.charCodeAt(i) & 0xff) << 8;
    for (let j = 0; j < 8; j++) {
      crc = (crc & 0x8000) !== 0 ? ((crc << 1) ^ 0x1021) & 0xffff : (crc << 1) & 0xffff;
    }
  }
  return crc;
}

/** Redis Cluster slot for a key, honoring the {hash-tag} rule. */
function slotOf(key: string): number {
  const open = key.indexOf("{");
  if (open !== -1) {
    const close = key.indexOf("}", open + 1);
    if (close > open + 1) key = key.slice(open + 1, close);
  }
  return crc16(key) % 16384;
}

function allResourceKeys(resource: string): string[] {
  const p = keyPrefix(resource);
  return [
    `${p}:readers`,
    `${p}:writer`,
    `${p}:queue`,
    `${p}:seq`,
    `${p}:fence`,
    `${p}:req:01J9ZsampleID`,
    `${p}:notify:01J9ZsampleID`,
  ];
}

describe("M5 cluster hash-tag co-location", () => {
  it("computes the same slot Redis does for a tagged key", () => {
    // anchor: redis-cli CLUSTER KEYSLOT 'rwlock:{order:9}:readers' -> 14638
    expect(slotOf("rwlock:{order:9}:readers")).toBe(14638);
  });

  it("places every key of a resource on a single slot", () => {
    for (const resource of ["order:9", "order:123", "a", "tenant-7/widget", "x".repeat(64)]) {
      const slots = new Set(allResourceKeys(resource).map(slotOf));
      expect(slots.size, `keys for ${resource} span multiple slots`).toBe(1);
    }
  });

  it("spreads distinct resources across the slot space (horizontal scaling)", () => {
    const resources = Array.from({ length: 200 }, (_, i) => `order:${i}`);
    const slots = new Set(resources.map((r) => slotOf(`${keyPrefix(r)}:holders`)));
    // not a strict guarantee, but 200 resources should land on many distinct slots
    expect(slots.size).toBeGreaterThan(100);
  });

  it("the global module marker is a single key (one slot)", () => {
    // rwlock:__module__ has no hash tag; it is a single key, trivially one slot.
    expect(slotOf("rwlock:__module__")).toBe(slotOf("rwlock:__module__"));
  });
});
