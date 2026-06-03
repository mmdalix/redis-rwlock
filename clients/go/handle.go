package rwlock

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// Handle is a held lock. It carries the fencing token (enforce it at your resource!)
// and a Context that is cancelled the instant the lock is lost.
type Handle struct {
	Resource string
	Mode     Mode
	Token    string
	Fencing  int64

	owner        *RwLock
	leaseUntilMs atomic.Int64
	lease        time.Duration // configured lease (for the watchdog re-extend)
	ctx          context.Context
	cancel       context.CancelCauseFunc
	settled      atomic.Bool // release-or-loss happens once
}

func (l *RwLock) newHandle(resource string, mode Mode, token string, fencing, leaseUntilMs int64) *Handle {
	ctx, cancel := context.WithCancelCause(context.Background())
	h := &Handle{Resource: resource, Mode: mode, Token: token, Fencing: fencing, owner: l, ctx: ctx, cancel: cancel}
	h.leaseUntilMs.Store(leaseUntilMs)
	return h
}

// FencingToken returns the monotonic per-resource fencing token for this hold.
func (h *Handle) FencingToken() int64 { return h.Fencing }

// LeaseUntil is the current lease expiry (advances on extend).
func (h *Handle) LeaseUntil() time.Time { return time.UnixMilli(h.leaseUntilMs.Load()) }

// Context is cancelled the instant the lock is lost (context.Cause == ErrLockLost) or
// when the handle is released (context.Cause == context.Canceled). Pass it into your
// work so it stops the moment the lease is no longer yours.
func (h *Handle) Context() context.Context { return h.ctx }

// Release frees the lock and hands it to the next eligible waiter(s). Idempotent;
// releasing a token that is no longer held is a benign no-op.
func (h *Handle) Release(ctx context.Context) error {
	if !h.settled.CompareAndSwap(false, true) {
		return nil
	}
	err := h.owner.releaseToken(ctx, h.Resource, h.Token)
	h.cancel(context.Canceled)
	return err
}

func (h *Handle) loseLock() {
	if !h.settled.CompareAndSwap(false, true) {
		return
	}
	h.cancel(ErrLockLost)
}

// Extend renews the lease (never shortens it). Fails closed with ErrLockLost both when
// the server reports the token is gone and — per the safety margin (SPEC §9.2) — when
// we are already within the margin of expiry.
func (h *Handle) Extend(ctx context.Context, lease time.Duration) error {
	if h.settled.Load() {
		return ErrLockLost
	}
	margin := h.owner.cfg.extensionMargin.Milliseconds()
	if h.owner.serverNowMs() >= h.leaseUntilMs.Load()-margin {
		h.loseLock()
		return ErrLockLost
	}
	newUntil, err := h.owner.extendToken(ctx, h.Resource, h.Token, lease)
	if err != nil {
		if errors.Is(err, ErrLockLost) {
			h.loseLock()
		}
		return err
	}
	h.leaseUntilMs.Store(newUntil)
	return nil
}

// startWatchdog re-extends the lease at ~lease/3 until the handle is released or lost;
// on loss it cancels the handle's Context. Stops when ctx is done (no goroutine leak).
func (h *Handle) startWatchdog() {
	interval := h.lease / 3
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-h.ctx.Done():
				return
			case <-t.C:
				cctx, cancel := context.WithTimeout(context.Background(), h.lease)
				_ = h.Extend(cctx, h.lease) // a real loss -> loseLock -> ctx.Done -> exit next tick
				cancel()
			}
		}
	}()
}
