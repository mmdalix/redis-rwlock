// Optional, auto-detected keyspace-expiry subscriber (SPEC §10.3). When the user's
// Redis has `notify-keyspace-events` including expired keyevents, we run one extra
// connection subscribed to `__keyevent@*__:expired`. When a per-resource lease
// sentinel (rwlock:{r}:lease_expiry) expires, we run expire_and_grant(r) — promptly
// reclaiming a crashed holder out-of-band, without any per-waiter self-wake roundtrip.
// We NEVER call CONFIG SET; if events are off, the self-wake path (SPEC §10.2) covers it.

/** Minimal structural view of a node-redis connection used for pub/sub. */
export interface SubscriberConn {
  on(event: "error", cb: (err: unknown) => void): unknown;
  connect(): Promise<unknown>;
  pSubscribe(pattern: string, listener: (message: string, channel: string) => void): Promise<unknown>;
  close(): Promise<unknown>;
  destroy?(): void;
}

// Matches any rwlock key and captures the resource between the hash-tag braces:
// e.g. "rwlock:{order:123}:lease_expiry" -> "order:123".
const RESOURCE_RE = /^rwlock:\{(.+)\}:/;

export function parseResource(key: string): string | undefined {
  const m = RESOURCE_RE.exec(key);
  return m ? m[1] : undefined;
}

export class KeyspaceSubscriber {
  private conn?: SubscriberConn;
  private readonly inFlight = new Set<string>();
  private readonly makeConn: () => SubscriberConn;
  private readonly onResourceExpired: (resource: string) => Promise<void>;

  constructor(makeConn: () => SubscriberConn, onResourceExpired: (resource: string) => Promise<void>) {
    this.makeConn = makeConn;
    this.onResourceExpired = onResourceExpired;
  }

  get active(): boolean {
    return this.conn !== undefined;
  }

  async start(): Promise<void> {
    if (this.conn) return;
    const conn = this.makeConn();
    conn.on("error", () => {
      /* swallow: recovery degrades to the self-wake path */
    });
    await conn.connect();
    try {
      await conn.pSubscribe("__keyevent@*__:expired", (message) => this.handle(String(message)));
    } catch (err) {
      // pSubscribe denied (e.g. ACL) -> don't leak the connected socket.
      try {
        await conn.close();
      } catch {
        conn.destroy?.();
      }
      throw err;
    }
    this.conn = conn;
  }

  /** A key expired: if it's one of ours, nudge that resource (deduped while in flight). */
  private handle(key: string): void {
    const resource = parseResource(key);
    if (resource === undefined || this.inFlight.has(resource)) return;
    this.inFlight.add(resource);
    void Promise.resolve(this.onResourceExpired(resource))
      .catch(() => {
        /* best-effort; the lease is the ultimate backstop */
      })
      .finally(() => this.inFlight.delete(resource));
  }

  async stop(): Promise<void> {
    const conn = this.conn;
    this.conn = undefined;
    if (conn) {
      try {
        await conn.close();
      } catch {
        conn.destroy?.();
      }
    }
  }
}
