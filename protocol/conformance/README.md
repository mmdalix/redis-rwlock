# Conformance scenarios

Language-agnostic, sequential scenarios that every client port MUST pass against
the shared Lua (Spec §20.10). They are the cross-language credibility check: the
same JSON, the same Redis, the same outcomes regardless of client language.

> **Scope note (M0 seed):** these initial scenarios are *sequential* — one logical
> client executing steps in order — which is enough to pin down immediate-grant,
> queueing-then-timeout, hand-off on release, and fencing monotonicity. Genuinely
> concurrent and mixed-language scenarios arrive with the second language port (M7);
> concurrency is exercised today by the Node client's own test suite.

## File format

Each file is one scenario:

```jsonc
{
  "name": "human-readable-id",
  "steps": [
    // acquire; on "granted" the resulting handle is saved under `as`
    { "op": "acquire", "as": "h1", "resource": "r1", "mode": "write",
      "leaseMs": 30000, "waitMs": 1000, "expect": "granted" },

    // acquire expected to time out (no handle saved)
    { "op": "acquire", "resource": "r1", "mode": "write",
      "leaseMs": 30000, "waitMs": 200, "expect": "timeout" },

    // release a previously saved handle
    { "op": "release", "handle": "h1" },

    // assert the most recent grant's fencing token is strictly greater than another's
    { "op": "expectFencingGt", "handle": "h3", "than": "h1" }
  ]
}
```

Supported ops: `acquire`, `release`, `expectFencingGt`.
`expect` on `acquire` is one of `granted` | `timeout`.

A runner loads every `scenarios/*.json`, executes the steps in order against a
fresh resource namespace, and asserts each `expect`.
