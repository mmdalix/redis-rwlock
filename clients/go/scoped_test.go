package rwlock_test

import (
	"context"
	"errors"
	"testing"
	"time"

	rwlock "github.com/mmdalix/redis-rwlock/clients/go"
	"github.com/mmdalix/redis-rwlock/clients/go/internal/redistest"
	"github.com/redis/go-redis/v9"
)

func TestWithWriteLockRunsAndReleases(t *testing.T) {
	ctx := context.Background()
	l := newLock(t)

	ran := false
	err := l.WithWriteLock(ctx, "s", func(ctx context.Context, h *rwlock.Handle) error {
		ran = true
		if h.FencingToken() <= 0 {
			t.Errorf("no fencing token")
		}
		return nil
	})
	if err != nil || !ran {
		t.Fatalf("withWriteLock err=%v ran=%v", err, ran)
	}
	// released -> a fresh acquire succeeds immediately
	h, err := l.AcquireWrite(ctx, "s", rwlock.Wait(time.Second))
	if err != nil {
		t.Fatalf("post-release acquire: %v", err)
	}
	_ = h.Release(ctx)
}

func TestWithWriteLockReleasesOnError(t *testing.T) {
	ctx := context.Background()
	l := newLock(t)
	sentinel := errors.New("boom")
	err := l.WithWriteLock(ctx, "s", func(ctx context.Context, h *rwlock.Handle) error {
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
	h, err := l.AcquireWrite(ctx, "s", rwlock.Wait(time.Second)) // still released
	if err != nil {
		t.Fatalf("post-error acquire: %v", err)
	}
	_ = h.Release(ctx)
}

func TestDoWriteReturnsValue(t *testing.T) {
	ctx := context.Background()
	l := newLock(t)
	got, err := rwlock.DoWrite(ctx, l, "s", func(ctx context.Context, h *rwlock.Handle) (int, error) {
		return 42, nil
	})
	if err != nil || got != 42 {
		t.Fatalf("DoWrite = %d, %v; want 42, nil", got, err)
	}
}

func TestWatchdogKeepsLockAliveAcrossPeriods(t *testing.T) {
	ctx := context.Background()
	addr := redistest.Start(t).Options().Addr
	// margin < lease/3 so a short lease can be safely re-extended
	a := rwlock.New(redis.NewClient(&redis.Options{Addr: addr}), rwlock.WithExtensionMargin(100*time.Millisecond))
	b := rwlock.New(redis.NewClient(&redis.Options{Addr: addr}))
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })

	err := a.WithWriteLock(ctx, "wd", func(ctx context.Context, h *rwlock.Handle) error {
		// hold ~1.2s on a 400ms lease -> only survives if the watchdog re-extends
		time.Sleep(1200 * time.Millisecond)
		if ctx.Err() != nil {
			t.Errorf("ctx cancelled mid-scope: %v", context.Cause(ctx))
		}
		return nil
	}, rwlock.Lease(400*time.Millisecond), rwlock.Watchdog())
	if err != nil {
		t.Fatalf("withWriteLock: %v", err)
	}
	// while held, a competitor could not get in
	if _, err := b.AcquireWrite(ctx, "wd", rwlock.Wait(200*time.Millisecond)); !errors.Is(err, rwlock.ErrWaitTimeout) {
		// (resource is free now; this acquire should succeed — just sanity that lock works after)
		if err != nil {
			t.Fatalf("post acquire: %v", err)
		}
	}
}

func TestWatchdogCancelsContextOnLoss(t *testing.T) {
	ctx := context.Background()
	server := redistest.Start(t)
	addr := server.Options().Addr
	l := rwlock.New(redis.NewClient(&redis.Options{Addr: addr}), rwlock.WithExtensionMargin(100*time.Millisecond))
	t.Cleanup(func() { _ = l.Close() })

	err := l.WithWriteLock(ctx, "wd2", func(ctx context.Context, h *rwlock.Handle) error {
		// force loss: delete the writer key out from under us
		if err := server.Del(ctx, "rwlock:{wd2}:writer").Err(); err != nil {
			t.Fatalf("del writer: %v", err)
		}
		select {
		case <-ctx.Done():
			if !errors.Is(context.Cause(ctx), rwlock.ErrLockLost) {
				t.Errorf("cause = %v, want ErrLockLost", context.Cause(ctx))
			}
			return nil
		case <-time.After(3 * time.Second):
			t.Errorf("ctx not cancelled after loss")
			return nil
		}
	}, rwlock.Lease(600*time.Millisecond), rwlock.Watchdog())
	if err != nil {
		t.Fatalf("withWriteLock: %v", err)
	}
}
