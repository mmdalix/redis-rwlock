package rwlock

import (
	"context"
	"time"
)

// Handle is a held lock. It carries the fencing token (enforce it at your resource!)
// and the granted lease. In G2 it will also expose a Context() cancelled on lock loss.
type Handle struct {
	Resource   string
	Mode       Mode
	Token      string
	Fencing    int64
	LeaseUntil time.Time

	owner *RwLock
}

// FencingToken returns the monotonic per-resource fencing token for this hold.
func (h *Handle) FencingToken() int64 { return h.Fencing }

// Release frees the lock and immediately hands it to the next eligible waiter(s).
// Releasing a token that is no longer held is a benign no-op.
func (h *Handle) Release(ctx context.Context) error {
	return h.owner.releaseToken(ctx, h.Resource, h.Token)
}

// Extend renews the lease (never shortens it). Returns ErrLockLost if the lock is gone.
func (h *Handle) Extend(ctx context.Context, lease time.Duration) error {
	newUntil, err := h.owner.extendToken(ctx, h.Resource, h.Token, lease)
	if err != nil {
		return err
	}
	h.LeaseUntil = time.UnixMilli(newUntil)
	return nil
}
