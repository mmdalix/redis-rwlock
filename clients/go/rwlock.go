// Package rwlock is a distributed read/write lock over Redis — many readers or one
// writer — with FIFO fair queueing (no polling), leases, fencing tokens, and crash
// recovery. All lock logic lives in shared, atomic server-side Lua; this client is a
// thin wrapper over an existing go-redis client.
//
// G0 scope: raw AcquireWrite/AcquireRead/Release/Extend, the BLPOP mailbox wait loop
// with bounded self-wake, Functions-or-EVALSHA delivery, the version handshake, and
// fail-closed errors. The scoped API + watchdog (context-cancel-on-loss), keyspace
// recovery, Inspect, and observability arrive in later milestones.
package rwlock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	epsilon  = 50 * time.Millisecond // wake just past a boundary
	floorMs  = 50 * time.Millisecond // never block for less (avoids a 0 = infinite BLPOP / tight loops)
)

// defaultOwnerID is the auto "who holds it" identity (sanitized of token separators).
var defaultOwnerID = func() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		h = "host"
	}
	h = strings.NewReplacer(":", "_", "{", "_", "}", "_").Replace(h)
	return fmt.Sprintf("%s#%d", h, os.Getpid())
}()

// RwLock wraps a go-redis client. Safe for concurrent use.
type RwLock struct {
	client   redis.UniversalClient
	cfg      config
	blocking redis.UniversalClient

	mu          sync.Mutex
	initialized bool
	closed      bool
	delivery    delivery
	offsetMs    int64 // serverNowMs ≈ localNowMs + offsetMs
}

// New wraps an existing go-redis client (standalone, Cluster, or Ring).
func New(client redis.UniversalClient, opts ...Option) *RwLock {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}
	return &RwLock{client: client, cfg: cfg}
}

func keyPrefix(resource string) string { return "rwlock:{" + resource + "}" }

func newRequestID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return strconv.FormatInt(time.Now().UnixMilli(), 36) + "-" + hex.EncodeToString(b[:])
}

func msOf(d time.Duration) int64 { return d.Milliseconds() }

func (l *RwLock) serverNowMs() int64 { return time.Now().UnixMilli() + l.offsetMs }

// ensureReady performs one-time init: learn the server clock offset, run the handshake
// + install, and stand up the dedicated blocking pool. Retries on failure.
func (l *RwLock) ensureReady(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if l.initialized {
		return nil
	}

	t, err := l.client.Time(ctx).Result()
	if err != nil {
		return fmt.Errorf("%w: init TIME: %v", ErrBackendUnavailable, err)
	}
	serverMs := t.UnixMilli()
	l.offsetMs = serverMs - time.Now().UnixMilli()

	d, err := installAndHandshake(ctx, l.client, l.cfg, serverMs)
	if err != nil {
		return err // already a typed error (incompatible / unsupported / backend)
	}
	l.delivery = d
	l.blocking = newBlockingClient(l.client, l.cfg.blockingPoolSize)
	l.initialized = true
	return nil
}

// AcquireWrite acquires the exclusive write lock for resource.
func (l *RwLock) AcquireWrite(ctx context.Context, resource string, opts ...AcquireOption) (*Handle, error) {
	return l.acquire(ctx, ModeWrite, resource, opts)
}

// AcquireRead acquires a shared read lock for resource.
func (l *RwLock) AcquireRead(ctx context.Context, resource string, opts ...AcquireOption) (*Handle, error) {
	return l.acquire(ctx, ModeRead, resource, opts)
}

func (l *RwLock) resolveAcquire(opts []AcquireOption) (acquireOptions, error) {
	o := acquireOptions{
		lease:          l.cfg.defaultLease,
		wait:           l.cfg.defaultWait,
		fairness:       l.cfg.defaultFairness,
		maxReaderBatch: l.cfg.maxReaderBatch,
	}
	for _, fn := range opts {
		fn(&o)
	}
	if o.ownerID == "" {
		if l.cfg.requireOwnerID {
			return o, fmt.Errorf("rwlock: Owner is required (WithRequireOwnerID is set)")
		}
		o.ownerID = defaultOwnerID
	}
	return o, nil
}

func (l *RwLock) acquire(ctx context.Context, mode Mode, resource string, opts []AcquireOption) (*Handle, error) {
	if err := l.ensureReady(ctx); err != nil {
		return nil, err
	}
	o, err := l.resolveAcquire(opts)
	if err != nil {
		return nil, err
	}
	prefix := keyPrefix(resource)
	reqID := newRequestID()

	args := []interface{}{
		string(mode), msOf(o.lease), msOf(o.wait), reqID, o.ownerID, string(o.fairness),
		o.maxReaderBatch, msOf(l.cfg.notifyKeyTTL), msOf(l.cfg.requestKeyTTLGrace),
	}
	res, err := l.delivery.run(ctx, "acquire", []string{prefix}, args)
	if err != nil {
		return nil, fmt.Errorf("%w: acquire %s: %v", ErrBackendUnavailable, resource, err)
	}
	arr, ok := res.([]interface{})
	if !ok || len(arr) == 0 {
		return nil, fmt.Errorf("%w: acquire %s: unexpected reply %T", ErrBackendUnavailable, resource, res)
	}

	if toStr(arr[0]) == "GRANTED" {
		return l.newHandle(resource, Mode(toStr(arr[4])), toStr(arr[1]), toInt64(arr[2]), toInt64(arr[3])), nil
	}
	// QUEUED: [ , request_id, notify_key, wait_deadline_ms, next_wake_ms]
	notifyKey := toStr(arr[2])
	waitDeadline := toInt64(arr[3])
	nextWake := toInt64(arr[4])
	return l.waitForGrant(ctx, resource, prefix, reqID, notifyKey, waitDeadline, nextWake)
}

func (l *RwLock) waitForGrant(ctx context.Context, resource, prefix, reqID, notifyKey string, waitDeadline, nextWake int64) (*Handle, error) {
	for {
		if err := ctx.Err(); err != nil {
			l.cancelWait(context.WithoutCancel(ctx), prefix, reqID)
			return nil, err
		}
		remaining := waitDeadline - l.serverNowMs()
		if remaining <= 0 {
			return l.finishTimeout(ctx, resource, prefix, reqID, notifyKey)
		}

		// Block until the soonest boundary or our deadline; if a boundary already
		// passed, wake almost immediately (floor) to run maintenance.
		block := time.Duration(remaining) * time.Millisecond
		if nextWake > 0 {
			b := time.Duration(nextWake-l.serverNowMs())*time.Millisecond + epsilon
			if b < floorMs {
				b = floorMs
			}
			if b < block {
				block = b
			}
		}
		if block < floorMs {
			block = floorMs
		}

		vals, err := l.blocking.BLPop(ctx, block, notifyKey).Result()
		if err == redis.Nil {
			// woke with no grant -> run maintenance, refresh the boundary, re-block.
			r2, merr := l.delivery.run(ctx, "expire_and_grant", []string{prefix}, []interface{}{msOf(l.cfg.notifyKeyTTL)})
			if merr != nil {
				return nil, fmt.Errorf("%w: wait maintenance %s: %v", ErrBackendUnavailable, resource, merr)
			}
			if arr, ok := r2.([]interface{}); ok && len(arr) >= 3 {
				nextWake = toInt64(arr[2])
			}
			continue
		}
		if err != nil {
			if cerr := ctx.Err(); cerr != nil {
				l.cancelWait(context.WithoutCancel(ctx), prefix, reqID)
				return nil, cerr
			}
			return nil, fmt.Errorf("%w: wait %s: %v", ErrBackendUnavailable, resource, err)
		}
		if len(vals) >= 2 {
			return l.handleFromPayload(resource, vals[1])
		}
	}
}

// finishTimeout: last-instant reconciliation (SPEC §8.4) — drain our mailbox, else cancel.
func (l *RwLock) finishTimeout(ctx context.Context, resource, prefix, reqID, notifyKey string) (*Handle, error) {
	el, err := l.client.LPop(ctx, notifyKey).Result()
	if err == nil {
		return l.handleFromPayload(resource, el)
	}
	if err != redis.Nil {
		return nil, fmt.Errorf("%w: wait drain %s: %v", ErrBackendUnavailable, resource, err)
	}
	l.cancelWait(ctx, prefix, reqID)
	return nil, ErrWaitTimeout
}

func (l *RwLock) cancelWait(ctx context.Context, prefix, reqID string) {
	// best-effort reconciliation; the lease is the ultimate backstop.
	_, _ = l.delivery.run(ctx, "cancel_wait", []string{prefix}, []interface{}{reqID, msOf(l.cfg.notifyKeyTTL)})
}

func (l *RwLock) handleFromPayload(resource, payload string) (*Handle, error) {
	var g struct {
		Token        string `json:"token"`
		Fencing      int64  `json:"fencing"`
		LeaseUntilMs int64  `json:"lease_until_ms"`
		Mode         string `json:"mode"`
	}
	if err := json.Unmarshal([]byte(payload), &g); err != nil {
		return nil, fmt.Errorf("%w: bad grant payload for %s: %v", ErrBackendUnavailable, resource, err)
	}
	return l.newHandle(resource, Mode(g.Mode), g.Token, g.Fencing, g.LeaseUntilMs), nil
}

func (l *RwLock) newHandle(resource string, mode Mode, token string, fencing, leaseUntilMs int64) *Handle {
	return &Handle{Resource: resource, Mode: mode, Token: token, Fencing: fencing, LeaseUntil: time.UnixMilli(leaseUntilMs), owner: l}
}

func (l *RwLock) releaseToken(ctx context.Context, resource, token string) error {
	if err := l.ensureReady(ctx); err != nil {
		return err
	}
	if _, err := l.delivery.run(ctx, "release", []string{keyPrefix(resource)}, []interface{}{token, msOf(l.cfg.notifyKeyTTL)}); err != nil {
		return fmt.Errorf("%w: release %s: %v", ErrBackendUnavailable, resource, err)
	}
	return nil
}

func (l *RwLock) extendToken(ctx context.Context, resource, token string, lease time.Duration) (int64, error) {
	if err := l.ensureReady(ctx); err != nil {
		return 0, err
	}
	res, err := l.delivery.run(ctx, "extend", []string{keyPrefix(resource)}, []interface{}{token, msOf(lease)})
	if err != nil {
		return 0, fmt.Errorf("%w: extend %s: %v", ErrBackendUnavailable, resource, err)
	}
	arr, ok := res.([]interface{})
	if !ok || len(arr) == 0 {
		return 0, fmt.Errorf("%w: extend %s: unexpected reply %T", ErrBackendUnavailable, resource, res)
	}
	if toStr(arr[0]) == "LOST" {
		return 0, ErrLockLost
	}
	return toInt64(arr[1]), nil
}

// DeliveryMode reports the active Lua delivery ("functions" or "scripts") after init.
func (l *RwLock) DeliveryMode() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.delivery == nil {
		return ""
	}
	return l.delivery.mode()
}

// Close releases the dedicated blocking pool (never the user's client). Idempotent.
func (l *RwLock) Close() error {
	l.mu.Lock()
	l.closed = true
	l.initialized = false
	b := l.blocking
	l.blocking = nil
	l.mu.Unlock()
	if b != nil && b != l.client {
		return b.Close()
	}
	return nil
}

// newBlockingClient returns a dedicated client for blocking BLPOP waits so they never
// starve the user's pool (SPEC §15). Falls back to the shared client for non-*redis.Client.
func newBlockingClient(c redis.UniversalClient, size int) redis.UniversalClient {
	if rc, ok := c.(*redis.Client); ok {
		opt := rc.Options()
		clone := *opt
		clone.PoolSize = size
		return redis.NewClient(&clone)
	}
	return c
}

func toStr(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprint(x)
	}
}

func toInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	case []byte:
		n, _ := strconv.ParseInt(string(x), 10, 64)
		return n
	default:
		return 0
	}
}
